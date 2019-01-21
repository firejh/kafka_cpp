#include "server.h"


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
    KafkaPCB cb;
    std::string brokers = conf.get_brokers();
    std::string topic = conf.get_topic();
    std::vector<std::string> send_data_vec = conf.get_send_data();
    std::string err_info;
    std::cout << "brokers " << brokers << "\n";
    
    KafkaP kafka_p;
    if (!kafka_p.init(brokers.c_str(), topic.c_str(), err_info, "1000", &cb)) {
        std::cout << "kafka producer init err, " << err_info << "\n";
        exit(1);
    } 
    std::cout << "init over\n";

    char data[1024] = {0};

    while (running_) {
        usleep(conf.get_usleep());
        for (auto& data : send_data_vec) {
            if (!kafka_p.produce(data.c_str(), data.size(), err_info)) {
                std::cout << "produce err, " << err_info << "\n";
            } else {
                //std::cout << "produce success\n";
            }
        }
    }

    sleep(3);
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
