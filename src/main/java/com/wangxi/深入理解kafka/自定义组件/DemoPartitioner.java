package com.wangxi.深入理解kafka.自定义组件;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <Description>
 *
 * @author wangxi
 */
public class DemoPartitioner implements Partitioner {

    // 有很多线程用同一个DemoPartitioner对象
    private final AtomicInteger count = new AtomicInteger(0);

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitionInfos = cluster.partitionsForTopic(topic);
        int partitionNum = partitionInfos.size();
        if (null == keyBytes) {
            // 轮询
            return count.getAndIncrement() % partitionNum;
        }
        return Utils.toPositive(Utils.murmur2(keyBytes) % partitionNum);
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}

