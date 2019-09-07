package com.wangxi.深入理解kafka.再均衡;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.codehaus.jackson.map.deser.std.StringDeserializer;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * <Description>
 *
 * @author wangxi
 */
public class ConsumerTest03 {
    public static final String brokerList = "192.168.1.110:9092";
    public static final String topic = "topic-demo";
    public static Properties props = null;
    public static final String groupId = "group.demo";
    public static final AtomicBoolean isRunning = new AtomicBoolean(true);

    public static Properties initConfig() {
        props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer-demo03");
        // 关闭自动提交
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return props;
    }

    @Before
    public void init() {
        initConfig();
    }


    @Test
    public void test01() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        Map<TopicPartition, OffsetAndMetadata> currentOffset = new HashMap<>();
        consumer.subscribe(Arrays.asList(topic), new ConsumerRebalanceListener() {
            // 这个方法会在再均衡开始之前和消费者停止读取消息之后被调用。可以通过这个回调方法
            //来处理消费位移的提交， 以此来避免一些不必要的重复消费现象的发生。参数 partitions 表
            //示再均衡前所分配到的分区。
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                // store offset in DB
            }
            // 这个方法会在重新分配分区之后和消费者开始读取消费之前被调用 。参数 partitions 表
            //示再均衡后所分配到的分区 。
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                for (TopicPartition partition : partitions) {
                    // 均衡完成之后，从db读取偏移量
                    consumer.seek(partition, getOffsetFromDb(partition));
                }
            }
        });
        while (isRunning.get()) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                // to deal something


                currentOffset.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));
            }

            consumer.commitAsync(currentOffset, null);
        }
    }

    private int getOffsetFromDb(TopicPartition partition){
        // deal sth

        return 0;
    }
}

