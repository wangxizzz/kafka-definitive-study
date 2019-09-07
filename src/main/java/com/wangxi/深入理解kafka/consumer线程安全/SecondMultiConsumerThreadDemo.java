package com.wangxi.深入理解kafka.consumer线程安全;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.codehaus.jackson.map.deser.std.StringDeserializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * <Description>
 *
 * @author wangxi
 */
public class SecondMultiConsumerThreadDemo {
    public static final String brokerList = "192.168.1.110:9092";
    public static final String topic = "topic-demo";
    public static final String groupId = "group.demo";
    public static final AtomicBoolean isRunning = new AtomicBoolean(true);
    // 保存偏移量
    public static ConcurrentHashMap<TopicPartition, OffsetAndMetadata> offsetsMap = new ConcurrentHashMap<>();
    public static Properties initConfig() {
        Properties props = new Properties();
        props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer-demo05");
        // 打开自动提交
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        return props;
    }


    public static void main(String[] args) {
        Properties properties = initConfig();
        // 起一个消费者线程，在消息poll下来处理时，使用多线程处理，增加吞吐量
        // 它所呈现的结构
        //是通过消费者拉取分批次的消息，然后提交给多线程进行处理
        KafkaConsumerThread kafkaConsumerThread = new KafkaConsumerThread(properties, topic, Runtime.getRuntime().availableProcessors());
        kafkaConsumerThread.start();
    }

    public static class KafkaConsumerThread extends Thread {
        private KafkaConsumer<String, String> kafkaConsumer;
        private ExecutorService service;
        private int threadNum;

        public KafkaConsumerThread(Properties properties, String topic, int threadNum) {
            this.kafkaConsumer = new KafkaConsumer<String, String>(properties);
            this.kafkaConsumer.subscribe(Collections.singletonList(topic));
            this.threadNum = threadNum;
            this.service = new ThreadPoolExecutor(threadNum, threadNum,
                    0L, TimeUnit.MICROSECONDS,
                    new ArrayBlockingQueue<>(1000),
                    new ThreadPoolExecutor.CallerRunsPolicy());
        }

        @Override
        public void run() {
            try {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));
                if (!records.isEmpty()) {
                    service.submit(new RecordsHandler(records));
                }
                if (!offsetsMap.isEmpty()) {
                    kafkaConsumer.commitSync(offsetsMap);
                }
            } catch (Exception e) {

            } finally {
                kafkaConsumer.close();
            }
        }
    }

    /**
     * 这种方式可以横向扩展(增加消费者线程),还可以较少TCP连接
     * 缺点是，无法保证消息消费的顺序性，因此偏移量提交是一个难点
     */

    public static class RecordsHandler extends Thread {
        private ConsumerRecords<String, String> records;
        public RecordsHandler(ConsumerRecords<String, String> records) {
            this.records = records;
        }

        @Override
        public void run() {
            // 处理records
            Set<TopicPartition> partitions = records.partitions();
            for (TopicPartition partition : partitions) {
                List<ConsumerRecord<String, String>> tpRecords = this.records.records(partition);
                for (ConsumerRecord<String, String> record : tpRecords) {
                    // 处理record
                }
                long lastOffset = tpRecords.get(tpRecords.size() - 1).offset();
                // 防止位移提交覆盖
                if (!offsetsMap.containsKey(partition)) {
                    offsetsMap.put(partition, new OffsetAndMetadata(lastOffset + 1));
                } else {
                    long position = offsetsMap.get(partition).offset();
                    if (position < lastOffset + 1) {
                        offsetsMap.put(partition, new OffsetAndMetadata(lastOffset + 1));
                    }
                }
            }
        }
        /**
         * 这种情况仍然会导致消息消费丢失的情况。
         * 比如：一个分区包含很多消息，那么就会存在多个线程消费同一分区，
         * 线程A拉取了0-99,线程B拉取了100-200，然后线程B提交了offset，此时线程A挂了，那么就会
         * 造成消息丢失。
         * 可以通过滑动窗口解决。具体可以参照书P93.
         */
    }
}

