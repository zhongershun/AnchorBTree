#include "util/daset_debug_logger.h"

namespace daset{

DebugLogger::DebugLogger() : logLevel_(Level::INFO) {}

DebugLogger::~DebugLogger() {
    if(file_.is_open()){
        file_.close();
    }
}

DebugLogger& DebugLogger::getInstance(){
    static DebugLogger instance;
    return instance;
}

void DebugLogger::setLevel(Level level){
    logLevel_.store(level);
}

void DebugLogger::setOutputFile(const std::string& filename){
    std::lock_guard<std::mutex> lock(fileMutex_);
    if (file_.is_open()) {
        file_.close();
    }
    file_.open(filename, std::ios::out | std::ios::app);
}

void DebugLogger::disableFileOutput() {
    std::lock_guard<std::mutex> lock(fileMutex_);
    if (file_.is_open()) {
        file_.close();
    }
}

void DebugLogger::log(Level level, const std::string& message, const std::string& file, int line){
    if (level < logLevel_.load()) {
        return;
    }

    auto now = std::chrono::system_clock::now();
    auto now_time = std::chrono::system_clock::to_time_t(now);
    auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;

    std::stringstream ss;
    ss << std::put_time(std::localtime(&now_time), "%Y-%m-%d %H:%M:%S")
       << '.' << std::setfill('0') << std::setw(3) << now_ms.count()
       << " [" << levelToString(level) << "] ";

    if (!file.empty() && line != -1) {
        ss << file << ":" << line << " - ";
    }

    ss << message;

    std::string logMessage = ss.str();

    // 输出到控制台
    {
        std::lock_guard<std::mutex> lock(consoleMutex_);
        std::cout << logMessage << std::endl;
    }

    // 输出到文件
    {
        std::lock_guard<std::mutex> lock(fileMutex_);
        if (file_.is_open()) {
            file_ << logMessage << std::endl;
        }
    }
}

auto DebugLogger::levelToString(Level level) -> std::string{
    switch (level) {
        case Level::DEBUG:      return "DEBUG";
        case Level::INFO:       return "INFO";
        case Level::WARNING:    return "WARNING";
        case Level::ERROR:      return "ERROR";
        case Level::CRITICAL:   return "CRITICAL";
        default:                return "UNKNOWN";
    }
}


}