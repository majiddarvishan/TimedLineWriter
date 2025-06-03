#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <chrono>
#include <ctime>
#include <atomic>
#include <vector>
#include <deque>
#include <mutex>
#include <condition_variable>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <boost/lockfree/queue.hpp>
#include <liburing.h>

class TimedLineWriter {
public:
    TimedLineWriter(const std::string& filenamePrefix,
                    size_t maxBatchBytes,
                    size_t poolSize,
                    int maxBatchDelayMs,
                    unsigned int uringDepth = 32)
        : filenamePrefix(filenamePrefix),
          maxBatchBytes(maxBatchBytes),
          maxBatchDelayMs(maxBatchDelayMs),
          queue(1024),
          stringPool(poolSize),
          stopFlag(false),
          lineCount(0),
          writeOffset(0),
          fd(-1),
          uringDepth(uringDepth) {
        for (size_t i = 0; i < poolSize; ++i) {
            stringPool.push(new std::string);
        }
        io_uring_queue_init(uringDepth, &ring, 0);
        openNewFile();
        workerThread = std::thread(&TimedLineWriter::processQueue, this);
    }

    ~TimedLineWriter() {
        stopFlag = true;
        if (workerThread.joinable()) workerThread.join();
        closeFile();
        io_uring_queue_exit(&ring);
        std::string* ptr;
        while (stringPool.pop(ptr)) delete ptr;
    }

    bool write(const std::string& text, int timeoutMs = 50) {
        using namespace std::chrono;
        auto deadline = steady_clock::now() + milliseconds(timeoutMs);

        std::string* strPtr = nullptr;
        while (!stringPool.pop(strPtr)) {
            if (steady_clock::now() >= deadline)
                break;
            std::this_thread::sleep_for(milliseconds(1));
        }

        if (strPtr) {
            strPtr->assign(text);
            while (!queue.push(strPtr)) {
                if (steady_clock::now() >= deadline) {
                    strPtr->clear();
                    if (!stringPool.push(strPtr)) delete strPtr;
                    break;
                }
                std::this_thread::sleep_for(milliseconds(1));
            }
            return true;
        }

        std::unique_lock<std::mutex> lock(fallbackMutex);
        if (fallbackBuffer.size() < FALLBACK_MAX_SIZE) {
            fallbackBuffer.emplace_back(text);
            fallbackCV.notify_one();
            return true;
        }
        return false;
    }

private:
    void openNewFile() {
        closeFile();

        auto t = std::time(nullptr);
        tm localTime;
#ifdef _WIN32
        localtime_s(&localTime, &t);
#else
        localtime_r(&t, &localTime);
#endif
        char buffer[64];
        strftime(buffer, sizeof(buffer), "%Y%m%d_%H%M%S", &localTime);

        filename = filenamePrefix + "_" + buffer + ".log";
        fd = ::open(filename.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd == -1) {
            std::cerr << "Failed to open file." << std::endl;
            return;
        }

        openTime = std::chrono::steady_clock::now();
        openDay = localTime.tm_mday;
        lineCount = 0;
        writeOffset = 0;
    }

    void closeFile() {
        if (fd != -1) {
            ::close(fd);
            fd = -1;
        }
    }

    void manageFileRotation() {
        using namespace std::chrono;
        auto now = steady_clock::now();
        auto duration = duration_cast<seconds>(now - openTime).count();

        auto t = std::time(nullptr);
        tm localTime;
#ifdef _WIN32
        localtime_s(&localTime, &t);
#else
        localtime_r(&t, &localTime);
#endif

        if (lineCount >= MAX_LINES ||
            duration >= MAX_SECONDS ||
            localTime.tm_mday != openDay) {
            openNewFile();
        }
    }

    void submitWriteAsync(const std::string& data) {
        struct io_uring_sqe* sqe = io_uring_get_sqe(&ring);
        if (!sqe) return;

        char* buffer = new char[data.size()];
        memcpy(buffer, data.data(), data.size());

        io_uring_prep_write(sqe, fd, buffer, data.size(), writeOffset);
        io_uring_sqe_set_data(sqe, buffer);
        io_uring_submit(&ring);

        writeOffset += data.size();
    }

    void completeWrites() {
        struct io_uring_cqe* cqe;
        while (io_uring_peek_cqe(&ring, &cqe) == 0) {
            delete[] static_cast<char*>(io_uring_cqe_get_data(cqe));
            io_uring_cqe_seen(&ring, cqe);
        }
    }

    void processQueue() {
        using namespace std::chrono;

        std::ostringstream batchBuffer;
        size_t batchSizeBytes = 0;
        auto nextFlush = steady_clock::now() + milliseconds(maxBatchDelayMs);

        while (true) {
            std::string* strPtr = nullptr;
            bool gotLine = false;

            {
                std::unique_lock<std::mutex> lock(fallbackMutex);
                if (!fallbackBuffer.empty()) {
                    std::string fallbackStr = std::move(fallbackBuffer.front());
                    fallbackBuffer.pop_front();

                    if (!stringPool.pop(strPtr)) {
                        strPtr = new std::string(std::move(fallbackStr));
                    } else {
                        strPtr->assign(std::move(fallbackStr));
                    }
                    gotLine = true;
                }
            }

            if (!gotLine) gotLine = queue.pop(strPtr);

            bool shouldFlush = false;
            if (gotLine) {
                size_t lineSize = strPtr->size() + 1;
                if (batchSizeBytes + lineSize > maxBatchBytes && batchSizeBytes > 0) {
                    while (!queue.push(strPtr)) std::this_thread::yield();
                    shouldFlush = true;
                } else {
                    batchBuffer << *strPtr << '\n';
                    batchSizeBytes += lineSize;
                    ++lineCount;
                    strPtr->clear();
                    if (!stringPool.push(strPtr)) delete strPtr;
                }
            } else {
                std::this_thread::sleep_for(milliseconds(5));
            }

            if (steady_clock::now() >= nextFlush || batchSizeBytes >= maxBatchBytes)
                shouldFlush = true;

            if (shouldFlush && batchSizeBytes > 0) {
                manageFileRotation();
                std::string batchStr = batchBuffer.str();
                submitWriteAsync(batchStr);
                completeWrites();
                batchBuffer.str("");
                batchBuffer.clear();
                batchSizeBytes = 0;
                nextFlush = steady_clock::now() + milliseconds(maxBatchDelayMs);
            }

            if (stopFlag && queue.empty() && batchSizeBytes == 0 && fallbackBuffer.empty())
                break;
        }

        completeWrites();
    }

private:
    const std::string filenamePrefix;
    const size_t maxBatchBytes;
    const int maxBatchDelayMs;
    const unsigned int uringDepth;
    std::string filename;
    std::atomic<bool> stopFlag;
    std::thread workerThread;
    std::chrono::steady_clock::time_point openTime;
    int openDay;
    size_t lineCount;

    boost::lockfree::queue<std::string*> queue;
    boost::lockfree::queue<std::string*> stringPool;

    std::mutex fallbackMutex;
    std::condition_variable fallbackCV;
    std::deque<std::string> fallbackBuffer;

    int fd;
    size_t writeOffset;
    struct io_uring ring;

    static constexpr int MAX_LINES = 100000;
    static constexpr int MAX_SECONDS = 60 * 5;
    static constexpr size_t FALLBACK_MAX_SIZE = 4096;
};
