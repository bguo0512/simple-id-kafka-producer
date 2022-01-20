package com.hotstar.simpleIdkafkaproducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SimpleIdKafkaProducerApplication implements CommandLineRunner {
    private static Logger sysLog = LoggerFactory.getLogger("sysLog");
    private static Logger commonLog = LoggerFactory.getLogger("commonLog");

    @Value("${bootstrap.servers}")
    String bootstrapServers;

    public static void main(String[] args) {
        SpringApplication.run(SimpleIdKafkaProducerApplication.class, args);
    }

    protected String getKey() {
        return UUID.randomUUID().toString();
    }

    @Override
    public void run(String... args) throws Exception {

        if (args.length != 3) {
            return;
        }

        String countryCode = args[0];
        String topic = args[1];
        String messageFileName = args[2];

        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringSerializer");
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringSerializer");

        commonLog.info("topic: {}", topic);
        commonLog.info("country_code: {}", countryCode);
        commonLog.info("file: {}", messageFileName);

        KafkaProducer<String, String> producer = new KafkaProducer<>(configs);

        List<Header> headers =
            Collections.singletonList(new RecordHeader("COUNTRY_CODE", countryCode.getBytes()));

        try (BufferedReader br = new BufferedReader(new FileReader(messageFileName))) {
            String line;
            while ((line = br.readLine()) != null) {
                ProducerRecord<String, String> record =
                    new ProducerRecord<>(topic, null, getKey(), line, headers);
                System.out.println(record);
                producer.send(record, (recordMetadata, e) -> {
                    if (e != null) {
                        commonLog.error("Sending message {} failed", record.value(), e);
                        sysLog.error(record.value());
                    }
                });
            }
        } catch (Exception e) {
            commonLog.error("Failed", e);
        }

        producer.close();
    }
}
