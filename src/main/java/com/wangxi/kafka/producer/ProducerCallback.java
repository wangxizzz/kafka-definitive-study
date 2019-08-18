package com.wangxi.kafka.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * Created by wangxi on 2018/4/24 11:24
 */

public class ProducerCallback implements Callback {
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        if (e != null) {
            e.printStackTrace(); //如果发生异常,那么e不为null
        }
        System.out.println(recordMetadata.offset());
    }
}
