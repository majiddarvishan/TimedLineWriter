#pragma once

#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <thread>
#include <chrono>
#include <atomic>
#include <ctime>
#include <vector>
#include <memory>
#include <boost/lockfree/queue.hpp>

class TimedLineWriter {
public:
    TimedLineWriter(const std::string& baseFileName,
                    size_t maxLines, int maxSeconds,
                    size_t maxBatchBytes = 8192,
                    int maxBatchDelayMs = 100)
        : baseFileName(baseFileName),
          maxLines(maxLines),
          maxSeconds(maxSeconds),
          maxBatchBytes(maxBatchBytes),
          maxBatchDelayMs(maxBatchDelayMs),
          lineCount(0),
          fileIndex(0),
          stopFlag(false),
          queue(QUEUE_SIZE),
          stringPool(POOL_SIZE)
    {
        // Pre-allocate string pool
        for (size_t i = 0; i < POOL_SIZE; ++i) {
            auto ptr = std::make_unique<std::string>();
            stringPool.push(ptr.get());
            poolStorage.push_back(std::move(ptr));
        }

        workerThread = std::thread(&TimedLineWriter::processQueue, this);
    }

    void write(const std::string& text) {
        std::string* strPtr = nullptr;
        if (stringPool.pop(strPtr)) {
            strPtr->assign(text);
        } else {
            strPtr = new std::string(text);  // fallback
        }

        while (!queue.push(strPtr)) {
            std::this_thread::yield();
        }
    }

    void stop() {
        stopFlag = true;
        if (workerThread.joinable())
            workerThread.join();

        cleanupRemaining();
        closeFile();
    }

    ~TimedLineWriter() {
        stop();
    }

private:
    static constexpr size_t QUEUE_SIZE = 2048;
    static constexpr size_t POOL_SIZE  = 2048;

    std::string baseFileName;
    size_t maxLines;
    int maxSeconds;
    size_t maxBatchBytes;
    int maxBatchDelayMs;

    size_t lineCount;
    int fileIndex;
    std::ofstream file;
    std::chrono::steady_clock::time_point fileStartTime;
    std::tm openDate = {};

    boost::lockfree::queue<std::string*> queue;
    boost::lockfree::queue<std::string*> stringPool;
    std::vector<std::unique_ptr<std::string>> poolStorage;

    std::atomic<bool> stopFlag;
    std::thread workerThread;

    void processQueue() {
        using namespace std::chrono;

        std::ostringstream batchBuffer;
        size_t batchSizeBytes = 0;
        auto nextFlush = steady_clock::now() + milliseconds(maxBatchDelayMs);

        while (true) {
            std::string* strPtr = nullptr;
            bool gotLine = queue.pop(strPtr);
            bool shouldFlush = false;

            if (gotLine) {
                size_t lineSize = strPtr->size() + 1;
                if (batchSizeBytes + lineSize > maxBatchBytes && batchSizeBytes > 0) {
                    // Delay until next flush
                    while (!queue.push(strPtr)) std::this_thread::yield();
                    shouldFlush = true;
                } else {
                    batchBuffer << *strPtr << '\n';
                    batchSizeBytes += lineSize;
                    ++lineCount;

                    strPtr->clear();
                    if (!stringPool.push(strPtr)) {
                        delete strPtr;
                    }
                }
            } else {
                std::this_thread::sleep_for(milliseconds(5));
            }

            if (steady_clock::now() >= nextFlush || batchSizeBytes >= maxBatchBytes)
                shouldFlush = true;

            if (shouldFlush && batchSizeBytes > 0) {
                manageFileRotation();
                file << batchBuffer.str();
                file.rdbuf()->pubsync();

                batchBuffer.str("");
                batchBuffer.clear();
                batchSizeBytes = 0;
                nextFlush = steady_clock::now() + milliseconds(maxBatchDelayMs);
            }

            if (stopFlag && queue.empty() && batchSizeBytes == 0)
                break;
        }
    }

    void manageFileRotation() {
        using namespace std::chrono;

        auto now = steady_clock::now();
        auto elapsed = duration_cast<seconds>(now - fileStartTime).count();
        std::time_t nowTime = std::time(nullptr);
        std::tm* currentDate = std::localtime(&nowTime);

        bool dayChanged = !file.is_open() ||
                          currentDate->tm_yday != openDate.tm_yday ||
                          currentDate->tm_year != openDate.tm_year;

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
        file.open(filename, std::ios::out | std::ios::binary);
        if (!file.is_open()) {
            std::cerr << "❌ Failed to open file: " << filename << std::endl;
        }

        lineCount = 0;
    }

    void closeFile() {
        if (file.is_open()) {
            file.rdbuf()->pubsync();
            file.close();
        }
    }

    void cleanupRemaining() {
        std::string* strPtr;
        while (queue.pop(strPtr)) {
            strPtr->clear();
            if (!stringPool.push(strPtr)) {
                delete strPtr;
            }
        }
    }
};
