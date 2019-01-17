/*
 * Created: 2019-01-15 15:46 +0800
 *
 * Modified: 2019-01-15 15:46 +0800
 *
 * Description: 
 * 1.发送时会判断kafka状态，避免kafka server关闭使用者不能感知，丢消息或者内存暴涨
 * 2.简单的实现kafka生产以及是否回调
 *
 * Author: jh
 */
#ifndef RDKAFKA_PRODUCER_KAFKA_P_H

#include <iostream>

#include "librdkafka/rdkafka.h"

//需要发送回调使用，一般发送不需要
class KafkaPCB
{
public:
    KafkaPCB() {};
    virtual ~KafkaPCB() {};

    //用户根据例子自己失效回调，可以打印日志
    virtual void p_cb(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque) {
        if (rkmessage->err) {
            std::cout << "kafka produce err, err_info=" 
            << rd_kafka_err2str(rkmessage->err) 
            << "\n";
        } else {
            std::cout << "kafka produce success, " 
            << "len="<< rkmessage->len
            << std::endl;
        }
        
    }
};

//生产者实现
class KafkaP
{
public:
    KafkaP();
    ~KafkaP();

    // des：初始化，该接口只能调用一次，否则会出现错误，且没有做多次init的防范
    // brokers,topic
    // err_info: 错误信息，正确为empty
    // cb: 回到设置，默认无回调，如果是重要消息，发送失败（server断开等异常）或者成功都会回调
    bool init(const char* brokers, const char* topic, std::string& err_info, KafkaPCB* cb = NULL);

    // 发送消息 
    // data: 消息内容
    // data_len: 消息长度
    // err_info: 错误信息，正确为empty，内部会clear
    // time_out：push的超时时间，0代表非阻塞
    bool produce(const char* data, uint16_t data_len, std::string& err_info, uint32_t time_out = 0);

private:
    static void produce_cb(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque);
private:
    //base
    std::string brokers_;
    std::string topic_;
    KafkaPCB* cb_;

    //rd
    rd_kafka_t* rdhandler_;
    rd_kafka_conf_t *rdconf_;

    //topic  
    rd_kafka_topic_t *rdtopic_;
    rd_kafka_topic_conf_t *rdtopic_conf_; 
};

#endif
