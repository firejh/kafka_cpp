#include <sstream>

#include "kafka_p.h"

KafkaP::KafkaP()
{
    status_ = false;
    cb_ = NULL;
    rdhandler_ = NULL;
    rdconf_ = NULL;
    rdtopic_ = NULL;
    rdtopic_conf_ = NULL;
}

KafkaP::~KafkaP()
{}

void KafkaP::stop()
{
    status_ = false;

    rd_kafka_poll(rdhandler_, 1000);

    rd_kafka_topic_destroy(rdtopic_);
    rd_kafka_destroy(rdhandler_);

}

bool KafkaP::init(const char* brokers, const char* topic, std::string& err_info, const char* clear_time_out, KafkaPCB* cb)
{
    err_info.clear();
    char temp[1024] = {0};

    //base 
    brokers_ = brokers;
    topic_ = topic;
    if (brokers_.empty()) {
        err_info = "Brokers empty";
        return false;    
    }
    if (topic_.empty()) {
        err_info = "Topic empty";
        return false;
    }

    //conf create and set
    rdconf_ = rd_kafka_conf_new();

    if (rd_kafka_conf_set(rdconf_, 
                          "bootstrap.servers", 
                          brokers, 
                          temp,  
                          sizeof(temp)) != RD_KAFKA_CONF_OK){  
        err_info.append(temp);
        return false;
    }  
    if (NULL != cb) {
        cb_ = cb;
        //设置回调，无返回类型
        rd_kafka_conf_set_dr_msg_cb(rdconf_, KafkaP::produce_cb);   
    }

    //handler create and set
    rdhandler_ = rd_kafka_new(RD_KAFKA_PRODUCER, rdconf_, temp, sizeof(temp));
    if (NULL == rdhandler_) {
        err_info.append(temp);
        return false;
    }
    if (rd_kafka_brokers_add(rdhandler_, brokers) == 0) {
        err_info = std::string("rd_kafka_brokers_add err, brokers=") + brokers;
        return false;
    }

    //topic conf
    rdtopic_conf_ = rd_kafka_topic_conf_new();
    /*
     * queue.buffering.max.messages，队列可以容纳的最大消息，默认100000，这里不支持自己设置
     * message.timeout.ms 300000，发送的消息发送成功立刻回调，如果不成功不会立刻回调，需要超时后回调返回失败，这里设置的就是超时时间ms
     */
    if (rd_kafka_topic_conf_set(rdtopic_conf_, 
                                "message.timeout.ms",
                                clear_time_out, 
                                temp, 
                                sizeof(temp)) != RD_KAFKA_CONF_OK) {
        err_info.append(temp);
        return false; 
    }

    //topic，如果不存在会自动创建topic
    rdtopic_ = rd_kafka_topic_new(rdhandler_, topic, rdtopic_conf_);
    
    status_ = true;

    return true;
}

bool KafkaP::produce(const char* data, uint16_t data_len, std::string& err_info, const char* key, uint16_t key_len, uint32_t time_out)
{
    if (false == status_) {
        rd_kafka_poll(rdhandler_, time_out);
        err_info = "KafkaP status is stopped, can not produce";
        return false;
    }
    err_info.clear();

    if (NULL == data || data_len == 0) {
        err_info = "data is NULL";
        return false;
    }

    int i = 0;
    int ret = rd_kafka_produce(rdtopic_, 
                               RD_KAFKA_PARTITION_UA,   //选取partition，这里是随机
                               RD_KAFKA_MSG_F_COPY,     //拷贝
                               (void*)data,             //消息内容
                               (size_t)data_len,        //消息长度
                               key,                     //key
                               key_len,                 //key_len
                               this                     //自己的回调数据指针
                               );

    if (0 != ret) {
        std::stringstream ss;
        ss << std::string("produce err, errno=") << errno
        << ", brokers=" << brokers_ 
        << ", topic=" << topic_;
        err_info = ss.str();
        rd_kafka_poll(rdhandler_, time_out);
        return false;
    }

    //可以理解为阻塞发送，time_out时间内，会等待该消息发送结果，time_out为0则是完全异步发送
    rd_kafka_poll(rdhandler_, time_out);

    return true;
}

void KafkaP:: produce_cb(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque)
{
    // opaque 总是null，怀疑是已经废弃的参数
    // rkmessage->_private 才是发送消息自己的数据指针
    if (rkmessage->_private == NULL) {
        std::cout << "rkmessage->_private = null, maybe producer have stopped\n";
        //pass，should not touch
    } else {
        KafkaP* self = (KafkaP*)rkmessage->_private;
        if (self->cb_ != NULL) {
            self->cb_->p_cb(rk, rkmessage, opaque);
        }
    }

    return;
}
