package com.wangxi.深入理解kafka.位移提交;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * <Description>
 *
 * @author wangxi
 */
@Slf4j
public class ConsumerTest02 {
    public static final String brokerList = "localhost:9092";
    public static final String topic = "topic-demo";
    public static Properties props = null;
    public static final String groupId = "group.demo02";
    public static final AtomicBoolean isRunning = new AtomicBoolean(true);

    public static Properties initConfig() {
        props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer-demo02");
        // 关闭自动提交
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
//        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
                props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    @Before
    public void init() {
        initConfig();
    }

    /**
     * 同步提交(批量提交)
     */
    @Test
    public void test01() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        // 订阅某个主题
        consumer.subscribe(Arrays.asList(topic));
        final int minBatchSize = 1;
        List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
        try {
            while (isRunning.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(record.topic() + "; " + record.offset());
                    buffer.add(record);
                }
                if (buffer.size() >= minBatchSize) {
                    // 批量处理这些消息


                    // 批量提交。提交的值为此次poll拉下来的位移+1
                    consumer.commitSync();
                    // 清空List
                    buffer.clear();
                }
            }
        } finally {
            consumer.close();
        }
        /**
         *  这种方式会发生重复消费的情况，在批量处理时，有一半的消息消费掉了，此时consumer挂了，那么
         *  偏移量就没提交
         */
    }

    /**
     * 按照分区粒度同步提交偏移量
     */
    @Test
    public void test02() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        try {
            while (isRunning.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (TopicPartition tp : records.partitions()) {
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(tp);
                    for (ConsumerRecord<String, String> record : partitionRecords) {
                        // 处理消息
                    }
                    // 提交单个分区偏移量
                    long lastConsumerOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                    consumer.commitSync(Collections.singletonMap(tp, new OffsetAndMetadata(lastConsumerOffset + 1)));
                }
            }
        } finally {
            consumer.close();
        }
    }

    /**
     * 异步提交
     */
    @Test
    public void test03 () {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        try {
            while (isRunning.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    // 处理
                }
                consumer.commitAsync(new OffsetCommitCallback() {
                    @Override
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                        if (exception == null) {
                            System.out.println(offsets);
                        } else {
                            log.error("fail to commit offsets {}", offsets, exception);
                        }
                    }
                });
            }
        } finally {
            consumer.close();
        }
    }
}

