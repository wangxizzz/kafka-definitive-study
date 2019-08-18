package com.wangxi.kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <Description>
 *  拦截key、value都为String的消息类型
 * @author wangxi
 */
public class MyProducerInterceptor implements ProducerInterceptor<String, String> {

    private AtomicInteger successSend = new AtomicInteger(0);
    private AtomicInteger failureSend = new AtomicInteger(0);

    // 生产者拦截器既可以用来在消息发送前做一些准备工作，
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        // 将value转化为大写
        return new ProducerRecord<>(record.topic(), record.partition(),
                record.timestamp(), record.key(),
                record.value().toUpperCase(), record.headers());
    }

    /**
     * KafkaProducer 会在消息被应答（ Acknowledgement ）之前或消息发送失败时调用生产者拦
     * 截器的onAcknowledgement()方法，优先于用户设定的Callback 之前执行。这个方法运行在
     * Producer 的I/O 线程中，所以这个方法中实现的代码逻辑越简单越好， 否则会影响消息的发送
     * 速度。
     */
    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (exception != null) {
            failureSend.addAndGet(1);
        } else {
            successSend.addAndGet(1);
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}

