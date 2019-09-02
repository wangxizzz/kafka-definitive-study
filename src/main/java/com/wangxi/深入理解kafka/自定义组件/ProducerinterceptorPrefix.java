package com.wangxi.深入理解kafka.自定义组件;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * <Description>
 *
 * @author wangxi
 */
public class ProducerinterceptorPrefix implements ProducerInterceptor<String, String> {
    // 需要保证线程安全性
    private volatile long sendSuccess = 0;
    private volatile long sendFailure = 0;

    // KafkaProducer 在将消息序列 化和计算分区 之前会调 用 生产者拦截器 的 onSend()方法来对消
    //息进行相应 的定制化操作。一般来说最好不要修改消息 ProducerRecord 的 topic 、 key和partition信息
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return null;
    }

    // KafkaProducer 会在消息被应答（ Acknowledgement ）之前或消息发送失败时调用生产者拦
    //截器的 onAcknowledgement（）方法，优先于用户设定的 Callback 之前执行。这个方法运行在
    //Producer 的 I/O 线程中，所以这个方法中实现的代码逻辑越简单越好 ， 否则会影响消息的发送
    //速度。
    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    //close （）方法主要用于在关闭拦截器时执行一些资源的清理工作。在这 3 个方法中抛 出的异
    //常都会被捕获并记录到日志中，但并不会再向上传递。
    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}

