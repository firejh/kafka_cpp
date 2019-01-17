#include "kafka_p.h"

KafkaP::KafkaP()
{
    cb_ = NULL;
    rdhandler_ = NULL;
    rdconf_ = NULL;
    rdtopic_ = NULL;
    rdtopic_conf_ = NULL;
}

KafkaP::~KafkaP()
{}

bool KafkaP::init(const char* brokers, const char* topic, std::string& err_info, KafkaPCB* cb)
{
    err_info.clear();
    err_info.resize(1024);

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
                          (char*)err_info.data(),  
                          err_info.size()) != RD_KAFKA_CONF_OK){  
        return false;
    }  
    if (NULL != cb) {
        cb_ = cb;
        //设置回调，无返回类型
        rd_kafka_conf_set_dr_msg_cb(rdconf_, produce_cb);   
    }

    //handler create and set
    rdhandler_ = rd_kafka_new(RD_KAFKA_PRODUCER, rdconf_, (char*)err_info.data(), err_info.size());
    if (NULL == rdhandler_) {
        return false;
    }
    if (rd_kafka_brokers_add(rdhandler_, brokers) == 0) {
        err_info = std::string("rd_kafka_brokers_add err, brokers=") + brokers;
        return false;
    }

    //topic conf
    rdtopic_conf_ = rd_kafka_topic_conf_new();
    if (rd_kafka_topic_conf_set(rdtopic_conf_, 
                                "message.timeout.ms",
                                "500", 
                                (char*)err_info.data(), 
                                err_info.size()) != RD_KAFKA_CONF_OK) {
        return false;    
    }

    //topic
    rdtopic_ = rd_kafka_topic_new(rdhandler_, topic, rdtopic_conf_);
    

    return true;
}

bool KafkaP::produce(const char* data, uint16_t data_len, std::string& err_info, uint32_t time_out)
{
    err_info.clear();
    err_info.resize(1024);

    if (NULL == data || data_len == 0) {
        err_info = "data is NULL";
        return false;
    }

    int ret = rd_kafka_produce(rdtopic_, 
                               RD_KAFKA_PARTITION_UA,   // 选取partition，这里是随机
                               RD_KAFKA_MSG_F_COPY,     //拷贝
                               (void*)data,             //消息内容
                               (size_t)data_len,        //消息长度
                               NULL,                    //key
                               0,                       //key_len
                               NULL                     //自己的回调数据指针
                               );

    if (-1 == ret) {
        //rd_kafka_err2str,这里会警告该用法过时，待完善 
        rd_kafka_resp_err_t err_t = rd_kafka_errno2err(errno);
        err_info = std::string("produce err, err_info=") + std::string(rd_kafka_err2str(err_t)) 
        + ", brokers=" + brokers_ 
        + ", topic=" + topic_;
        rd_kafka_poll(rdhandler_, time_out);
        return false;
    }

    rd_kafka_poll(rdhandler_, time_out);

    return true;
}

void KafkaP:: produce_cb(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque)
{
    std::cout << "call back\n";
    if (rkmessage->err) {
        std::cout << "kafka produce err, err_info="
        << rd_kafka_err2str(rkmessage->err)
        << "\n";
    } else {
        std::cout << "kafka produce success, "
        << "len="<< rkmessage->len
        << std::endl;
    }
    //cb_->p_cb(rk, rkmessage, opaque);
}
