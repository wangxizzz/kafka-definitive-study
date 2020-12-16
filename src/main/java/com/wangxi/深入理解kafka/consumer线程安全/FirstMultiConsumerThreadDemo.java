package com.wangxi.深入理解kafka.consumer线程安全;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.codehaus.jackson.map.deser.std.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * <Description>
 *KafkaConsumer 非线程安全并不意 味着我们在消费消息的时候只能以单线程的方式执行。
 * @author wangxi
 */
public class FirstMultiConsumerThreadDemo {
    public static final String brokerList = "192.168.1.110:9092";
    public static final String topic = "topic-demo";
    public static final String groupId = "group.demo";
    public static final AtomicBoolean isRunning = new AtomicBoolean(true);

    public static Properties initConfig() {
        Properties props = new Properties();
        props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer-demo04");
        // 关闭自动提交
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return props;
    }

    public static void main(String[] args) {
        Properties props = initConfig();
        /**
         *  起4个消费者线程(相当于起了4个消费者在同一个consumerGroup内)，如果分区数少于4，那么会造成线程空闲的状态。
         *
         *  这种 多线程的实现方式和开启多个消费进程的方式没有本质上的区别，它的优点是每个线程可以按顺序消费各个分区中的消息。
         *  缺点也很明显，每个消费线程都要维护一个独立的TCP连接 。
         */

        int consumerThreadNum = 4;
        for (int i = 0; i < consumerThreadNum; i++) {
            new KafkaConsumerThread(props, topic).start();
        }
    }

    public static class KafkaConsumerThread extends Thread {
        private KafkaConsumer<String, String> kafkaConsumer;

        public KafkaConsumerThread(Properties props, String topic) {
            kafkaConsumer = new KafkaConsumer<String, String>(props);
            kafkaConsumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            try {
                while (isRunning.get()) {
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));
                    for (ConsumerRecord<String, String> record : records) {
                        // 处理消息
                    }
                    // 偏移量处理
                }
            } catch (Exception e) {

            } finally {
                kafkaConsumer.close();
            }
        }
    }
}

