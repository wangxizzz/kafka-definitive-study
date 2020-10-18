package com.wangxi.深入理解kafka.消费者;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

/**
 * <Description>
 *
 * @author wangxi
 */
@Slf4j
public class ConsumerTest01 {
    public static final String brokerList = "localhost:9092";
    public static final String topic = "topic-demo";
    public static Properties props = null;
    public static final String groupId = "group.demo01";
    public static final AtomicBoolean isRunning = new AtomicBoolean(true);

    public static Properties initConfig() {
        props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer-demo01");
        // 打开自动提交
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
//        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        return props;
    }

    @Before
    public void init() {
        initConfig();
    }

    /**
     * 订阅主题
     */
    @Test
    public void test01 () {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        // 订阅某个主题
        consumer.subscribe(Arrays.asList(topic));
        // 或者传入正则
//        consumer.subscribe(Pattern.compile("topic-.*"));
        try {
            while (isRunning.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                // 如果拉取的分区为空，那么会返回空集合
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(record.topic() + "; " + record.offset());
                }
            }
        } catch (Exception e) {
            log.error("", e);
        } finally {
            consumer.close();
        }
    }

    /**
     * assign订阅分区
     */
    @Test
    public void test02() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        List<TopicPartition> partitions = new ArrayList<>();
        // 获取主题所有分区信息
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
        if (partitionInfos != null || !partitionInfos.isEmpty()) {
            for (PartitionInfo info : partitionInfos) {
                partitions.add(new TopicPartition(info.topic(), info.partition()));
            }
        }
        consumer.assign(partitions);
    }

    /**
     * 从分区的维度消费消息
     * ConsumerRecord表示一条消息的记录
     */
    @Test
    public void test03() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
        // 有可能一个消费者会消费多个分区
        for (TopicPartition partition : records.partitions()) {
            for (ConsumerRecord<String, String> record : records.records(partition)) {
                System.out.println(record.topic() + "," + record.value());
            }
        }
    }

    /**
     * 按照主题维度消费消息
     */
    @Test
    public void test04() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        List<String> topicList = Arrays.asList(topic, topic);
        consumer.subscribe(topicList);
        try {
            while (isRunning.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                // 统计消息的个数
                System.out.println(records.count());
                // 判断消息集是否为空
                System.out.println(records.isEmpty());
                for (String tp : topicList) {
                    // 按照主题维度消费
                    for (ConsumerRecord<String, String> record : records.records(tp)) {
                        System.out.println(record.topic() + "," + record.value());
                    }
                }
            }
        } finally {
            consumer.close();
        }
    }
}

