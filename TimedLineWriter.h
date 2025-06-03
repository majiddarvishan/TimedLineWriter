#include <iostream>
#include <fstream>
#include <string>
#include <thread>
#include <queue>
#include <vector>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <ctime>
#include <atomic>

class TimedLineWriter {
public:
    TimedLineWriter(const std::string& baseFileName,
                    size_t maxLines, int maxSeconds,
                    size_t maxBatchBytes = 1024,
                    int maxBatchDelayMs = 500)
        : baseFileName(baseFileName), maxLines(maxLines), maxSeconds(maxSeconds),
          maxBatchBytes(maxBatchBytes), maxBatchDelayMs(maxBatchDelayMs),
          lineCount(0), fileIndex(0), stopFlag(false)
    {
        workerThread = std::thread(&TimedLineWriter::processQueue, this);
    }

    void write(const std::string& text) {
        {
            std::lock_guard<std::mutex> lock(queueMutex);
            queue.push(text);
        }
        queueCond.notify_one();
    }

    void stop() {
        {
            std::lock_guard<std::mutex> lock(queueMutex);
            stopFlag = true;
        }
        queueCond.notify_all();
        if (workerThread.joinable()) {
            workerThread.join();
        }
    }

    ~TimedLineWriter() {
        stop();
        closeFile();
    }

private:
    std::string baseFileName;
    size_t maxLines;
    int maxSeconds;
    size_t maxBatchBytes;
    int maxBatchDelayMs;

    size_t lineCount;
    int fileIndex;
    std::ofstream file;
    std::chrono::steady_clock::time_point fileStartTime;
    std::tm openDate;

    std::queue<std::string> queue;
    std::mutex queueMutex;
    std::condition_variable queueCond;
    std::thread workerThread;
    std::atomic<bool> stopFlag;

    void processQueue() {
        using namespace std::chrono;
        std::vector<std::string> batch;
        size_t batchSizeBytes = 0;
        auto lastFlush = steady_clock::now();

        while (true) {
            std::unique_lock<std::mutex> lock(queueMutex);
            queueCond.wait_for(lock, milliseconds(maxBatchDelayMs), [&] {
                return !queue.empty() || stopFlag;
            });

            while (!queue.empty()) {
                const auto& str = queue.front();
                size_t newSize = batchSizeBytes + str.size() + 1; // +1 for '\n'
                if (newSize > maxBatchBytes && !batch.empty())
                    break;  // Don't overflow the current batch

                batch.push_back(str);
                batchSizeBytes = newSize;
                queue.pop();
            }

            if (stopFlag && queue.empty() && batch.empty()) break;

            lock.unlock();

            auto now = steady_clock::now();
            bool timeExceeded = duration_cast<milliseconds>(now - lastFlush).count() >= maxBatchDelayMs;

            if (!batch.empty() && (batchSizeBytes >= maxBatchBytes || timeExceeded)) {
                manageFileRotation();

                for (const auto& line : batch) {
                    file << line << '\n';
                    ++lineCount;
                }

                file.flush();
                batch.clear();
                batchSizeBytes = 0;
                lastFlush = now;
            }
        }

        // Final flush on shutdown
        if (!batch.empty()) {
            manageFileRotation();
            for (const auto& line : batch) {
                file << line << '\n';
                ++lineCount;
            }
            file.flush();
        }
    }

    void manageFileRotation() {
        using namespace std::chrono;

        auto now = steady_clock::now();
        auto elapsed = duration_cast<seconds>(now - fileStartTime).count();
        std::time_t currentTime = std::time(nullptr);
        std::tm* nowTm = std::localtime(&currentTime);

        bool dayChanged = !file.is_open() ||
                          nowTm->tm_yday != openDate.tm_yday ||
                          nowTm->tm_year != openDate.tm_year;

        if (!file.is_open() || lineCount >= maxLines || elapsed >= maxSeconds || dayChanged) {
            closeFile();
            openNewFile();
        }
    }

    void openNewFile() {
        fileStartTime = std::chrono::steady_clock::now();
        std::time_t now = std::time(nullptr);
        openDate = *std::localtime(&now);

        std::string filename = baseFileName + "_" + std::to_string(fileIndex++) + ".txt";
        file.open(filename, std::ios::out);
        lineCount = 0;

        if (!file.is_open()) {
            std::cerr << "Failed to open file: " << filename << std::endl;
        }
    }

    void closeFile() {
        if (file.is_open()) {
            file.flush();
            file.close();
        }
    }
};
