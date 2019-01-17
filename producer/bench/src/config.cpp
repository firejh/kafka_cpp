#include <sstream>
#include <iostream>
#include "config.h"

Config::Config()
{

}

Config::~Config()
{
}

void Config::load(const char* path)
{
    dictionary* ini = iniparser_load(path);
    if (NULL == ini) {
        //throw;
        exit(1);
        return;
    }

    process_name_ = iniparser_getstring(ini, "server:process_name", "meta_server");

    log_sys_file_ = iniparser_getstring(ini, "spdlog:sys_file", "/data/logs/sys_file");
    log_logic_file_ = iniparser_getstring(ini, "spdlog:logic_file", "/data/logs/logic_file");
    log_sys_level_ = iniparser_getint(ini, "spdlog:sys_level", 0);
    log_logic_level_ = iniparser_getint(ini, "spdlog:logic_level", 0);
    log_max_size_ =  iniparser_getint(ini, "spdlog:log_file_size", 1000);

    brokers_ = iniparser_getstring(ini, "kafka:brokers", "1");
    topic_ = iniparser_getstring(ini, "kafka:topic", "");
    usleep_ = iniparser_getint(ini, "kafka:usleep", 1000000);

    uint32_t send_num = iniparser_getint(ini, "kafka:send_num", 0);
    for (int i = 0; i < send_num; ++i) {
        std::stringstream ss;
        ss << "kafka:send_data_" << (i + 1);
        std::string tmp = iniparser_getstring(ini, ss.str().c_str(), "");
        if (!tmp.empty()) {
            send_data_.push_back(tmp);
        }
    }
    std::cout << "send_data_ size=" << send_data_.size() << "\n";

    iniparser_freedict(ini);
    return;
}

void Config::reload(const char* path)
{
    dictionary* ini = NULL;
    ini = iniparser_load(path);
    if (NULL == ini) {
        return;
    }

    iniparser_freedict(ini);

    return;
}
