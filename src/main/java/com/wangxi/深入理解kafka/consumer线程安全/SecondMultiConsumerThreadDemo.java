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
         * 线程A拉取了0-99,线程B拉取了100-200，然后线程B提交了offset，此时线程A并未处理完就挂了，那么就会
         * 造成消息丢失，因为此时会从 200偏移量进行消费。
         *
         *
         * 可以通过滑动窗口解决：
         *
         * 每一个方格代表一个批次的消息，一个滑动窗口包含若干方格，startOffset标注的是当前滑动窗口的起始位置，endOffset标注的是末尾位置。
         * 每当startOffset指向的方格中的消息被消费完成，就可以提交这部分的位移，与此同时，窗口向前滑动一格，
         * 删除原来startOffset所指方格中对应的消息，并且拉取新的消息进入窗口。
         * 滑动窗口的大小固定，所对应的用来暂存消息的缓存大小也就固定了，这部分内存开销可控。
         * 方格大小和滑动窗口的大小同时决定了消费线程的并发数：一个方格对应一个消费线程，对于窗口大小固定的情况，
         * 方格越小并行度越高；对于方格大小固定的情况，窗口越大并行度越高。
         * 不过，若窗口设置得过大，不仅会增大内存的开销，而且在发生异常（比如Crash）的情况下也会引起大量的重复消费，
         * 同时还考虑线程切换的开销，建议根据实际情况设置一个合理的值，不管是对于方格还是窗口而言，过大或过小都不合适。
         *
         * 如果一个方格内的消息无法被标记为消费完成，那么就会造成 startOffset 的悬停。
         * 为了使窗口能够继续向前滑动，那么就需要设定一个阈值，当 startOffset 悬停一定的时间后就对这部分消息进行本地重试消费，
         * 如果重试失败就转入重试队列，如果还不奏效就转入死信队列，。
         * 真实应用中无法消费的情况极少，一般是由业务代码的处理逻辑引起的，比如消息中的内容格式与业务处理的内容格式不符，
         * 无法对这条消息进行决断，这种情况可以通过优化代码逻辑或采取丢弃策略来避免。如果需要消息高度可靠，
         * 也可以将无法进行业务逻辑的消息（这类消息可以称为死信）存入磁盘、数据库或Kafka，
         * 然后继续消费下一条消息以保证整体消费进度合理推进，之后可以通过一个额外的处理任务来分析死信进而找出异常的原因
         *
         */
    }
}

