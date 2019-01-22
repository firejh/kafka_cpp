#ifndef BCS_LOGGER_H
#define BCS_LOGGER_H

#include <string>

#include "spdlog/spdlog.h"
#include "spdlog/async_logger.h"
#include "spdlog/sinks/file_sinks.h"
#include "spdlog/sinks/null_sink.h"

#define SPD_LOG_QUEUQE_SIZE (1024 * 32)

using namespace spdlog;

//创建一个自己的logger对象，不能直接使用spd的get，是加锁操作
static std::shared_ptr<logger> create_hourly_logger(const std::string logger_name, std::string out_file)
{
    auto logger =  spdlog::hourly_logger_mt(logger_name, out_file);
    logger->set_pattern("%v");
    return logger;
}

static std::shared_ptr<logger> create_daily_logger(const std::string logger_name, std::string out_file)
{
    auto logger =  spdlog::daily_logger_mt(logger_name, out_file);
    logger->set_pattern("[%H:%M:%S,thread %t] %v");
    return logger;
}

/*
    trace = 0,
    debug = 1,
    info = 2,
    warn = 3,
    err = 4,
    critical = 5,
    off = 6 
*/
//设置日志等级
static void set_logger_level(std::shared_ptr<logger> logger, level::level_enum log_level)
{
    logger->set_level(log_level);
}

static void set_async_mode(uint32_t queque_size = SPD_LOG_QUEUQE_SIZE)
{
    set_async_mode(queque_size, async_overflow_policy::discard_log_msg/*失败扔掉*/, nullptr, std::chrono::milliseconds(10), nullptr);
}

#define SYS_LOG_TRACE(format, args...)\
sys_log->trace((std::string("[TRACE][{}:{}] ") + std::string(format)).c_str(), __FILE__, __LINE__, ##args)

#define SYS_LOG_DEBUG(format, args...)\
sys_log->debug((std::string("[DEBUG][{}:{}] ") + std::string(format)).c_str(), __FILE__, __LINE__, ##args)

#define SYS_LOG_INFO(format, args...)\
sys_log->info((std::string("[INFO][{}:{}] ") + std::string(format)).c_str(), __FILE__, __LINE__, ##args)

#define SYS_LOG_WARN(format, args...)\
sys_log->warn((std::string("[WARN][{}:{}] ") + std::string(format)).c_str(), __FILE__, __LINE__, ##args)

#define SYS_LOG_ERROR(format, args...)\
sys_log->error((std::string("[ERROR][{}:{}] ") + std::string(format)).c_str(), __FILE__, __LINE__, ##args)


#endif
