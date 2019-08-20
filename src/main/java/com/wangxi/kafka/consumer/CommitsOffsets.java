package com.wangxi.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by wangxi on 2018/4/25 15:24
 */

/*
 */
public class CommitsOffsets {

    private void commitCurrentOffset() {
        while (true) {
            ConsumerRecords<String, String> records = ConsumerTest.kafkaConsumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("topic=" + record.topic() + " partition=" + record.partition() + " offset=" + record.offset() + " key=" + record.key() + " value=" + record.value());
            }
            try {
                //会产生阻塞
                ConsumerTest.kafkaConsumer.commitSync();//处理完当前batch中的数据提交当前数据中最大的offset然后在获取新数据
            } catch (CommitFailedException e) { //当发生可恢复的错误时 这个方法会retry
                System.out.println("commit failed...");
            }
        }
    }


    public void asynchronousCommit() {
        while (true) {
            ConsumerRecords<String, String> records = ConsumerTest.kafkaConsumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("topic=" + record.topic() + " partition=" + record.partition() + " offset=" + record.offset() + " key=" + record.key() + " value=" + record.value());
            }
            //简单的异步提交  commit的顺序不能保证
            ConsumerTest.kafkaConsumer.commitAsync();//commit the last offset and carry on 也会重试
            //回调
            ConsumerTest.kafkaConsumer.commitAsync(new OffsetCommitCallback() {
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception e) {
                    if (e != null) {
                        System.out.println("commit failed for offsets " + offsets); //可以打印日志或者发送到dashboard展示
                    }
                }
            });
        }
    }

    /**
     * combining synchronous and asynchronous commits
     * 其实偶尔的失败不是一个很大的问题,这个问题可能是占时的,以后的提交还是会成功的 但是在关闭consumer或者发生rebalance时要明确的给出最新的offset
     * 因此可以结合同步和异步来完成关闭consumer前的提交(发生rebalance时下面在说)
     */
    public void syncAndAsync() {
        try {
            while (true) {
                ConsumerRecords<String, String> records = ConsumerTest.kafkaConsumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("topic=" + record.topic() + " partition=" + record.partition() + " offset=" + record.offset() + " key=" + record.key() + " value=" + record.value());
                }

                ConsumerTest.kafkaConsumer.commitAsync();//没有错误发生时使用异步提交因为快 失败了 下一个提交会retry
            }
        } catch (Exception e) {
            System.out.println("Unexpectes error");
        } finally {
            try {
                ConsumerTest.kafkaConsumer.commitSync(); //当关闭consumer时,没有下一个commit, 所以使用同步提交,因为它会重试直到成功提交或者遇到不可重试错误时
            } finally {
                ConsumerTest.kafkaConsumer.close();
            }
        }
    }

    /**
     * 通过partition维度去提交offset
     */
    public void commitOffsetByPartition() {
        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = ConsumerTest.kafkaConsumer.poll(Duration.ofMillis(100));
                for (TopicPartition partition : consumerRecords.partitions()) {
                    List<ConsumerRecord<String, String>> records = consumerRecords.records(partition);
                    // 消费该分区的消息
                    for (ConsumerRecord<String, String> record : records) {
                        // 可以在其他线程处理消息（防止阻塞消费者线程）
                    }
                    long latConsumerOffset = records.get(records.size() - 1).offset();
                    ConsumerTest.kafkaConsumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(latConsumerOffset + 1)));
                }
            }
        } finally {
            ConsumerTest.kafkaConsumer.close();
        }
    }

}
