package com.rankey.kafka.streams;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class BankTransactionsProducerTest {

    @Test
    public void newRandomTransactionsTest() {
        ProducerRecord<String, String> record = BankTransactionsProducer.newRandomTransaction("jhon");
        String key = record.key();
        String value = record.value();

        Assert.assertEquals(key, "jhon");
        System.out.println(value);

        ObjectMapper mapper = new ObjectMapper();
        try {
            JsonNode node = mapper.readTree(value);
            Assert.assertEquals(node.get("name").asText(), "jhon");
            Assert.assertTrue("Amount should be less than 100", node.get("amount").asInt() < 100);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
