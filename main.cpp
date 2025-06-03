#include "TimedLineWriter.h"

int main() {
    TimedLineWriter writer("perf_log", 1000000, 3600, 8192, 100); // 8 KB batch, 100 ms flush window

    for (int i = 0; i < 100000; ++i) {
        writer.write("This is line number " + std::to_string(i));
    }

    writer.stop();
    return 0;
}
