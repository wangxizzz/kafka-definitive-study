package com.wangxi.深入理解kafka.生产者;

import com.wangxi.深入理解kafka.自定义组件.DemoPartitioner;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * <Description>
 *
 * @author wangxi
 */
@Slf4j
public class ProducerTest01 {
    public static final String brokerList = "192.168.1.110:9092";
    public static final String topic = "topic-demo";
    public static Properties props = null;
    // 多线程访问安全
    public static KafkaProducer<String, String> producer = null;

    @Before
    public void init() {
        props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        // 获取类的全路径名
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "producer01");
        // 发生可重试异常时，重试次数
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        // 指定自定义分区。因为producer是多线程访问安全的，因此就要求自定义分区的线程安全
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, DemoPartitioner.class.getName());
        producer = new KafkaProducer<>(props);
    }

    @Test
    public void testSyncSend() {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, "hello kafka");
        try {
            RecordMetadata recordMetadata = producer.send(record).get();
            System.out.println(recordMetadata.topic());
            System.out.println(recordMetadata.partition());
            System.out.println(recordMetadata.offset());
        } catch (ExecutionException | InterruptedException e) {
            // 进一步处理异常后的消息
            log.error("同步发送消息失败", e);
        }
    }

    @Test
    public void testAsync() throws InterruptedException {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, "hello kafka");
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception != null) {
                    // 需要处理消息
                    log.error("", exception);
                } else {
                    System.out.println(metadata.topic() + ":" + metadata.offset());
                }
            }
        });
        Thread.currentThread().join();
    }
}

