package com.wangxi.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.Future;

/**
 * Created by wangxi on 2018/4/18 16:27
 */

public class KafkaProducerTest {
    private static Properties properties = new Properties();
    private static KafkaProducer<String, String> producer = null;

    static {
        ClassLoader classLoader = KafkaProducerTest.class.getClassLoader();
        InputStream inputStream = classLoader.getResourceAsStream("kafkaproducer.properties");
        try {
            properties.load(inputStream);
            producer = new KafkaProducer<>(properties);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 这种方法会把消息放入buffer中,由单独的线程负责发送
     * send()返回Future对象但是这里没有处理,也就是不知道消息是否发送成功
     * 这种方式在可容忍丢失消息的情况下可以使用,生产一般不用
     */
    private void unSaveProducer() {
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("java_topic", "name", "hello world!");
        for (int i = 0; i < 100; i++) {
            try {
                producer.send(record);
                record = new ProducerRecord<String, String>("java_topic", "name", "hello world--" + i);
                System.out.println(record);
                producer.flush();
            } catch (Exception e) {
                e.printStackTrace();
                //尽管没有处理发送到kafka的异常但是别的异常也可能会发生比如
                // SerializationException BufferExhaustedException TimeoutException InterruptException(发送线程被中断)
            }
        }
    }

    /**
     * 同步发送
     */
    private void synchronousProducer() {
        ProducerRecord<String, String> record = new ProducerRecord<>("java_topic", "name", "Rilley");
        try {
            Future<RecordMetadata> future = producer.send(record);
            future.get(); //wait for a reply from kafka,如果没有成功抛出异常,成功返回元信息
            System.out.println("返回结果：" + future.get());
            RecordMetadata recordMetadata = future.get();
            System.out.println(recordMetadata.offset());
            System.out.println(recordMetadata.partition());//获取分区信息
            System.out.println(recordMetadata.topic()); // 获取主题信息
        } catch (Exception e) {
            e.printStackTrace(); //其它异常
        }
    }

    /**
     * 假设网络往返(app--kafka)一次需要10ms,那么如果等待每条消息的reply,发送100条消息要1s.
     * 如果不需要reply那么效率会更高,大多数情况并不需要
     * reply
     * 如果既要异步也要处理错误,可以使用回调
     * 异步发送要flush()
     */
    private void asynchronousProducer() {
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("java_topic", "name", "wangxi");
        producer.send(record, new ProducerCallback());  // 自定义回调类implements CallBack接口
        producer.flush();
    }

    public static void main(String[] args) {
        KafkaProducerTest kafkaProducerTest = new KafkaProducerTest();
        kafkaProducerTest.unSaveProducer();
//        kafkaProducerTest.synchronousProducer();
//        kafkaProducerTest.asynchronousProducer();
    }
}