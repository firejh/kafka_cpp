#include <signal.h>
#include <sys/stat.h>
#include <iostream>

#include "global.h"
#include "server.h"
#include "logger.h"

Config conf;
std::shared_ptr<logger> sys_log = nullptr;
std::shared_ptr<logger> st_log = nullptr;

volatile bool exit_flag = true;

void sig_handler(int sig)
{
    std::cout << "get sig " << sig << "\n";
    switch (sig) {
    case SIGPIPE:
        SYS_LOG_ERROR("Received SIGPIPE scheduling shutdown... signal={}", sig);
        break;
    case SIGINT:
        SYS_LOG_INFO("Received SIGINT scheduling shutdown... signal={}", sig);
        break;
    case SIGTERM:
        SYS_LOG_INFO("Received SIGTERM scheduling shutdown... signal={}", sig);
        break;
    default:
        SYS_LOG_INFO("Received shutdown signal, scheduling shutdown... signal={}", sig);
    }
    exit_flag = false;
    std::cout << "exit_flag false\n";
}

void set_signal_handlers(void)
{
    struct sigaction act;
    sigemptyset(&act.sa_mask);
    act.sa_flags = 0;
    act.sa_handler = sig_handler;
    sigaction(SIGTERM, &act, NULL);
    sigaction(SIGINT, &act, NULL);
}

std::string config_file;
void check()
{
    sleep(3);
    while (1) {
        sleep(3);
        conf.reload(config_file.c_str());
    }
}

int main(int argc, char* argv[])
{
    try {
        //argv
        if (argc < 2 ) {
            printf("illegal argc, please input config file");
            return -1;
        }

        //config
        config_file = argv[1];
        dictionary* d = iniparser_load(config_file.c_str());
        if (NULL == d) {
            printf("open config %s failed", config_file.c_str());
            return -1;
        }
        conf.load(config_file.c_str());

        //log
        set_async_mode();
        sys_log = create_daily_logger("log_level_sys", conf.get_sys_log_file());
        sys_log->set_level(level::level_enum(conf.get_sys_log_level()));
        st_log = create_hourly_logger("log_level_logic", conf.get_logic_log_file());
        st_log->set_level(level::level_enum(conf.get_logic_log_level()));

        std::thread(check).detach();

        //signal
        signal(SIGHUP, SIG_IGN);
        signal(SIGPIPE, SIG_IGN);
        set_signal_handlers();


        //work start
        Server s;
        s.open();

        //wait stop
        while (exit_flag) {
            sleep(1);
        }
        std::cout << "close..\n";

        //work stop
        s.close();
    } catch (std::exception& e) {
        return -1;
    }

    return 0;
}
