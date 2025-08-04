#include <iostream>
#include <fstream>
#include <string>
#include <mutex>
#include <memory>
#include <chrono>
#include <iomanip>
#include <sstream>
#include <atomic>

namespace daset{

class DebugLogger {
public:
    enum class Level {
        DEBUG,
        INFO,
        WARNING,
        ERROR,
        CRITICAL
    };

    // 获取单例实例
    static auto getInstance() -> DebugLogger&;
    // static DebugLogger& getInstance();

    // 设置日志级别
    void setLevel(Level level);

    // 设置输出文件
    void setOutputFile(const std::string& filename);

    // 关闭文件输出
    void disableFileOutput();

    // 日志输出接口
    void log(Level level, const std::string& message, const std::string& file, int line);

    // 删除拷贝构造函数和赋值运算符
    DebugLogger(const DebugLogger&) = delete;
    DebugLogger& operator=(const DebugLogger&) = delete;

private:
    DebugLogger();
    ~DebugLogger();

    auto levelToString(Level level) -> std::string;

    std::ofstream file_;
    std::mutex fileMutex_;
    std::mutex consoleMutex_;
    std::atomic<Level> logLevel_;
};

// 宏定义简化日志调用
#define LOG_DEBUG(message)    DebugLogger::getInstance().log(DebugLogger::Level::DEBUG, message, __FILE__, __LINE__)
#define LOG_INFO(message)     DebugLogger::getInstance().log(DebugLogger::Level::INFO, message, __FILE__, __LINE__)
#define LOG_WARNING(message)  DebugLogger::getInstance().log(DebugLogger::Level::WARNING, message, __FILE__, __LINE__)
#define LOG_ERROR(message)    DebugLogger::getInstance().log(DebugLogger::Level::ERROR, message, __FILE__, __LINE__)
#define LOG_CRITICAL(message) DebugLogger::getInstance().log(DebugLogger::Level::CRITICAL, message, __FILE__, __LINE__)

}