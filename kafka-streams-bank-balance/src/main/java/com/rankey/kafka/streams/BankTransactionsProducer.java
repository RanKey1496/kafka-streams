package com.rankey.kafka.streams;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public class BankTransactionsProducer {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "3");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        Producer<String, String> producer = new KafkaProducer<String, String>(properties);
        int i = 0;
        while (true) {
            System.out.println("Producing batch: " + i);
            try {
                producer.send(newRandomTransaction("jhon"));
                Thread.sleep(100);
                producer.send(newRandomTransaction("manuela"));
                Thread.sleep(100);
                producer.send(newRandomTransaction("mary"));
                Thread.sleep(100);
                i += 1;
            } catch (InterruptedException e) {
                break;
            }
        }
    }

    public static ProducerRecord<String, String> newRandomTransaction(String name) {
        ObjectNode transaction = JsonNodeFactory.instance.objectNode();
        Integer amount = ThreadLocalRandom.current().nextInt(0, 100);
        Instant now = Instant.now();
        transaction.put("name", name);
        transaction.put("amount", amount);
        transaction.put("time", now.toString());
        return new ProducerRecord<>("bank-transaction", name, transaction.toString());
    }

}
