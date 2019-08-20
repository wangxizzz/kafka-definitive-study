package com.wangxi.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * Created by wangxi on 2018/4/25 10:03
 */

//同一个线程中不能有多个同组的消费者同时多线程下也不能使用同一个consumer, one consumer per thread is the rule

public class ConsumerTest {

    public static Properties properties = new Properties();
    public static KafkaConsumer<String, String> kafkaConsumer = null;

    static {
        try {
            properties.load(ConsumerTest.class.getClassLoader().getResourceAsStream("kafkaconsumer.properties"));
            kafkaConsumer = new KafkaConsumer<String, String>(properties);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void consumer() {
        kafkaConsumer.subscribe(Collections.singletonList("java_topic")); //可以有多个topic 可以是正则表达式(一旦一个新topic符合正则,rebalance立即触发)
//        kafkaConsumer.subscribe(Pattern.compile("test.*"));
        try {
            while (true) {
                //consumer必须不断的poll msgs否则就会认为consumer挂了
                //poll不仅仅是取数据 如果是第一次消费(group leader)还要负责分配partition
                // 发生rebalance在poll中处理分配partition的过程
                //还负责发生心跳(新版本不是这样),所有处理消息的过程应足够高效且快
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));//如果没有数据到consumer buffer 阻塞多久
                //应该在别的线程处理消息
                if (consumerRecords.count() == 0) {
                    System.out.println("consumerRecords.count() = 0");
                    break;
                }
                for (ConsumerRecord<String, String> record : consumerRecords) {
                    // record.timestamp()表示消息的发送时间，而不是消息的接收时间
                    System.out.println("+++++++++++++++++++++++++++++++++++++++++topic=" + record.timestamp() + record.topic() + " partition=" + record.partition() + " offset=" + record.offset() + " key=" + record.key() + " value=" + record.value());
                }
            }
        } finally {
            kafkaConsumer.close();
            //关闭连接,并且立即触发rebalance,而不是等着group coordinator发现consumer挂了才rebalance(会有一段间隔导致无法消费消息)
        }
    }

    public static void main(String[] args) {
        ConsumerTest consumerTest = new ConsumerTest();
        consumerTest.consumer();
    }
}
