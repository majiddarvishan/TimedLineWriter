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
#include <sys/mman.h>
#include <sys/stat.h>
#include <boost/lockfree/queue.hpp>

class TimedLineWriter {
public:
    TimedLineWriter(const std::string& filenamePrefix,
                    size_t maxBatchBytes,
                    size_t poolSize,
                    int maxBatchDelayMs)
        : filenamePrefix(filenamePrefix),
          maxBatchBytes(maxBatchBytes),
          maxBatchDelayMs(maxBatchDelayMs),
          queue(1024),
          stringPool(poolSize),
          stopFlag(false),
          lineCount(0),
          mappedSize(0),
          writeOffset(0),
          fd(-1),
          mappedData(nullptr) {
        for (size_t i = 0; i < poolSize; ++i) {
            stringPool.push(new std::string);
        }
        openNewFile();
        workerThread = std::thread(&TimedLineWriter::processQueue, this);
    }

    ~TimedLineWriter() {
        stopFlag = true;
        if (workerThread.joinable()) workerThread.join();
        closeFile();
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
        fd = ::open(filename.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
        if (fd == -1) {
            std::cerr << "Failed to open file." << std::endl;
            return;
        }

        mappedSize = 10 * 1024 * 1024;  // 10MB
        if (ftruncate(fd, mappedSize) == -1) {
            std::cerr << "Failed to set file size." << std::endl;
            ::close(fd);
            fd = -1;
            return;
        }

        mappedData = static_cast<char*>(mmap(nullptr, mappedSize, PROT_WRITE, MAP_SHARED, fd, 0));
        if (mappedData == MAP_FAILED) {
            std::cerr << "Failed to mmap file." << std::endl;
            ::close(fd);
            fd = -1;
            mappedData = nullptr;
            return;
        }

        openTime = std::chrono::steady_clock::now();
        openDay = localTime.tm_mday;
        lineCount = 0;
        writeOffset = 0;
    }

    void closeFile() {
        if (mappedData && mappedData != MAP_FAILED) {
            msync(mappedData, writeOffset, MS_SYNC);
            munmap(mappedData, mappedSize);
            mappedData = nullptr;
        }
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
                if (writeOffset + batchStr.size() < mappedSize) {
                    memcpy(mappedData + writeOffset, batchStr.data(), batchStr.size());
                    writeOffset += batchStr.size();
                    msync(mappedData, writeOffset, MS_SYNC);
                }
                batchBuffer.str("");
                batchBuffer.clear();
                batchSizeBytes = 0;
                nextFlush = steady_clock::now() + milliseconds(maxBatchDelayMs);
            }

            if (stopFlag && queue.empty() && batchSizeBytes == 0 && fallbackBuffer.empty())
                break;
        }
    }

private:
    const std::string filenamePrefix;
    const size_t maxBatchBytes;
    const int maxBatchDelayMs;
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
    char* mappedData;
    size_t mappedSize;
    size_t writeOffset;

    static constexpr int MAX_LINES = 100000;
    static constexpr int MAX_SECONDS = 60 * 5;
    static constexpr size_t FALLBACK_MAX_SIZE = 4096;
};
