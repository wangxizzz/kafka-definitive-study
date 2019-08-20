package com.wangxi.kafka.serializer;

import org.apache.kafka.common.serialization.Deserializer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * <Description>
 *
 * @author wangxi
 */
public class CustomerDeserializer implements Deserializer<Customer> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public Customer deserialize(String topic, byte[] data) {
        if (data == null || data.length == 0) {
            return null;
        }
        ByteBuffer wrap = ByteBuffer.wrap(data);
        int customerID = wrap.getInt();
        int customerNameLength = wrap.getInt();
        byte[] customerNameBytes = new byte[customerNameLength];
        wrap.get(customerNameBytes);
        String customerName = new String(customerNameBytes, StandardCharsets.UTF_8);
        return new Customer(customerID, customerName);
    }

    @Override
    public void close() {

    }
}

