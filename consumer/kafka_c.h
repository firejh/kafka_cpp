/*
 * Created: 2019-01-21 17:46 +0800
 *
 * Modified: 2019-01-21 17:46 +0800
 *
 * Description: 
 *
 * Author: jh
 */

#ifndef rd_kafka_CONSUMER_KAFKA_C_H
#define rd_kafka_CONSUMER_KAFKA_C_H

#include "string.h"
#include <string>

#include "librdkafka/rdkafka.h"

class KafkaC
{
public:
    KafkaC();
    ~KafkaC();

    // des: 初始化
    // return: 是否有错误
    // brokers、topic、group
    // err_info: 出错的话返回错误
    // consume_old: 只在该group没有消费过（commit记录不存在）的情况下可用，false从最新的开始消费，true从最早的消息开始消费
    // others: 如果单消费者关闭很长时间，启动时候不想消费这段时间的数据需要换一个group继续消费
    bool init(const char* brokers, const char* topic, std::string group, std::string& err_info, bool consume_old = false);

    // 消费kafka消息，有消息一直读即可，没有的话内部会阻塞time_cout时间
    // return: 是否获取消息，超时后也是返回false，需要根据err_info是否为空判断是否报错
    // msg: 获取的消息
    // err_info: 出错的话返回错误
    // time_cout: 没有消息时的阻塞时间ms
    bool consume(std::string& msg, std::string& err_info, uint32_t time_cout = 200);

private:
    bool status_;
    std::string brokers_;
    std::string topic_;
    std::string group_;

    rd_kafka_t* rd_kafka_;
    rd_kafka_conf_t *rd_kafka_conf_;

    rd_kafka_topic_t *rd_topic_;
    rd_kafka_topic_conf_t *rd_topic_conf_;

    rd_kafka_topic_partition_list_t* rd_topic_partition_list_;
    rd_kafka_topic_partition_t* rd_topic_partition_;
};

#endif
