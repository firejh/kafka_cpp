#include <sstream>
#include "kafka_c.h"

#include <iostream>
KafkaC::KafkaC()
{
    status_ = false;
}

KafkaC::~KafkaC()
{
    if (NULL != rd_topic_partition_list_) {
        rd_kafka_topic_partition_list_destroy(rd_topic_partition_list_);
        rd_topic_partition_list_ = NULL;
    }
    if (NULL != rd_kafka_) {
        rd_kafka_consumer_close(rd_kafka_); 
        rd_kafka_ = NULL;
    }

    rd_kafka_wait_destroyed(1000);
}

bool KafkaC::init(const char* brokers, const char* topic, std::string group, std::string& err_info, bool consume_old)
{
    char temp[1024] = {0};
    err_info.clear();
    if (true == status_) {
        err_info = "KafkaC reinit";
        return false;
    }
    if (brokers == NULL || topic == NULL || group.empty()) {
        err_info = std::string("kafka info err, brokers=") +  brokers + ", topic=" + topic + ", group=" + group;
        return false;
    }
    
    if (status_ == true) {
        err_info = "kafka consumer reinit";
        return false;
    }

    brokers_ = brokers;
    topic_ = topic;
    group_ = group;

    // topic conf
    rd_topic_conf_ = rd_kafka_topic_conf_new();
    std::string offset_reset_str = "largest";
    if (false == consume_old) {
        offset_reset_str = "smallest";
    }
    // 简单说就是本groupid初次消费的时候largest会从最新的开始消费，smallest会从最最早的消息开始消费
    if (RD_KAFKA_CONF_OK != rd_kafka_topic_conf_set(rd_topic_conf_, "auto.offset.reset", offset_reset_str.c_str(), temp, sizeof(temp))) {
        err_info.append(temp);
        return false;
    }
    // commit记录方式，默认的是broker
    if (RD_KAFKA_CONF_OK != rd_kafka_topic_conf_set(rd_topic_conf_, "offset.store.method", "broker", temp, sizeof(temp))) {
        err_info.append(temp);
        return false;
    }

    // kafka conf
    rd_kafka_conf_ = rd_kafka_conf_new();
    if (RD_KAFKA_CONF_OK != rd_kafka_conf_set(rd_kafka_conf_, "group.id", group_.c_str(), temp, sizeof(temp))) {
        err_info.append(temp);
        return false;
    }
    rd_kafka_conf_set_default_topic_conf(rd_kafka_conf_, rd_topic_conf_);

    // rd kafka
    rd_kafka_ = rd_kafka_new(RD_KAFKA_CONSUMER, rd_kafka_conf_, temp, sizeof(temp));
    if (NULL == rd_kafka_) {
        err_info.append(temp);
        return false;
    }
    // return 添加的broker数量
    if (0 == rd_kafka_brokers_add(rd_kafka_, brokers_.c_str())) {
        err_info = "rd_kafka_brokers_add err";
        return false;
    }
    if (RD_KAFKA_RESP_ERR_NO_ERROR != rd_kafka_poll_set_consumer(rd_kafka_)) {
        std::stringstream ss;
        ss <<"rd_kafka_poll_set_consumer err";
        err_info = ss.str();
        return false;
    }

    // 创建topic partion的存储链表，1为大小表示装入一个ropic的数据
    rd_topic_partition_list_ = rd_kafka_topic_partition_list_new(1);
    if (rd_topic_partition_list_ == NULL) {
        err_info = "rd_kafka_topic_partition_list_new err";
        return false;
    }
    rd_topic_partition_ = rd_kafka_topic_partition_list_add(rd_topic_partition_list_, topic_.c_str(), -1);
    if (NULL == rd_topic_partition_) {
        err_info = "rd_kafka_topic_partition_list_add err";
        return false;
    }
    if (RD_KAFKA_RESP_ERR_NO_ERROR != rd_kafka_subscribe(rd_kafka_, rd_topic_partition_list_)) {
        err_info = "rd_kafka_subscribe err";
        return false;
    }
    
    status_ = true;
    return true;
}

bool KafkaC::consume(std::string& msg, std::string& err_info, uint32_t time_cout)
{
    err_info.clear();
    rd_kafka_message_t* k_msg = rd_kafka_consumer_poll(rd_kafka_, time_cout);
    if (NULL == k_msg) {
        //如果没有消息超时返回null，不是错误
        return false;
    }

    bool ret;
    do {
        //错误
        if (RD_KAFKA_RESP_ERR_NO_ERROR != k_msg->err) {
            ret = false;
            if (RD_KAFKA_RESP_ERR__PARTITION_EOF == k_msg->err) {
                break;
            }

            err_info = rd_kafka_message_errstr(k_msg);
            break;
        }

    //std::cout << k_msg->partition << "\n";
        ret = true;
        msg.assign((char*)k_msg->payload, k_msg->len);
        break;

    } while(1);

    rd_kafka_message_destroy(k_msg);

    return ret;
}
