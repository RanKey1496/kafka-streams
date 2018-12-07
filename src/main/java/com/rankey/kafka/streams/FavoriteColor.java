package com.rankey.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

public class FavoriteColor {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "favorite-color-application");
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // We disable the cache to demonstrate all the "steps" involved in the transformation - not recommended in pro
        properties.setProperty(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> textLines = builder.stream("favorite-color-input");

        KStream<String, String> usersAndColors = textLines
                                                    .filter((key, value) -> value.contains(","))
                                                    .selectKey((key, value) -> value.split(",")[0].toLowerCase())
                                                    .mapValues(value -> value.split(",")[1].toLowerCase())
                                                    .filter((user, color) -> Arrays.asList("green", "blue", "red").contains(color));

        usersAndColors.to("favorite-color-middle", Produced.with(Serdes.String(), Serdes.String()));

        KTable<String, String> usersAndColorsTable = builder.table("favorite-color-middle");

        KTable<String, Long> favoriteColor = usersAndColorsTable
                                                .groupBy((user, color) -> new KeyValue<>(color, color))
                                                .count(Materialized.as("CountsByColors"));

        favoriteColor.toStream().to("favorite-color-output", Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), properties);

        streams.cleanUp();
        streams.start();

        streams.localThreadsMetadata().forEach(data -> System.out.println(data));
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }



}
