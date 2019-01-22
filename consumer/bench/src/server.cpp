#include "server.h"

#include <iostream>
#include <sstream>
#include <string>


Server::Server()
{
    running_ = false;
}

Server::~Server()
{
}

void Server::open()
{
    if (true == running_) return;

    running_ = true;

    std::thread t(std::bind(&Server::run, this));
    t.detach();

}

void Server::run()
{
    std::string brokers = conf.get_brokers();
    std::string topic = conf.get_topic();
    std::string group = conf.get_group();
    std::cout << "brokers " << brokers << "\n";
    
    KafkaC consumer;
    std::string err_info;
    if (false == consumer.init(brokers.c_str(), topic.c_str(), group, err_info)) {
        std::cout << "init err, " << err_info << "\n";
        exit(-1);
    }
    while (running_) {
        std::string msg;

        if (consumer.consume(msg, err_info)) {
            std::cout << "get msg: " << msg << "\n";
        } else {
            if (!err_info.empty()) {
                std::cout << "consume err: " << err_info << "\n";
            }
        }

    }

}

void Server::close()
{
    running_ = false;
    std::cout << "server close\n";
    /*
    for (auto th : threads_) {
        if (th->joinable()) {
            th->join();
        }
    }*/

    //stop
}
