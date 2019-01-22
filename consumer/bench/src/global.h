#ifndef __GLOBAL_H__
#define __GLOBAL_H__

#include "config.h"
#include "logger.h"

using namespace spdlog;
extern std::shared_ptr<logger> sys_log;
extern std::shared_ptr<logger> st_log;

extern Config conf;
#endif
