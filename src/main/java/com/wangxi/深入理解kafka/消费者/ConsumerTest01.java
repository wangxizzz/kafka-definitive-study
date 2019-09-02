package com.wangxi.深入理解kafka.消费者;

import org.junit.Test;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * <Description>
 *
 * @author wangxi
 */
public class ConsumerTest01 {
    public static final String brokerList = "192.168.1.110:9092";
    public static final String topic = "topic-demo";
    public static Properties props = null;
    public static final String groupId = "group.demo";
    public static final AtomicBoolean isRunning = new AtomicBoolean(true);

    public static Properties initConfig() {
        props = new Properties();

    }

    @Test
    public void test01 () {

    }
}

