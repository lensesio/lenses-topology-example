package io.lenses.topology.example.microservice;

import io.lenses.topology.client.TopologyClient;
import io.lenses.topology.client.kafka.metrics.KafkaPublisher;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class SuspiciousPaymentsServiceApp {
    private static final String suspiciousPayments = "suspicious_payments";

    public static void main(final String[] args) throws Exception {
        if (args.length != 1 || args[0].isEmpty()) {
            throw new IllegalArgumentException("args[0] should provider the Kafka Brokers");
        }

        final String brokers = args[0];

        final Properties kafkaProperties = new Properties();
        kafkaProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        kafkaProperties.put(KafkaPublisher.TOPOLOGY_TOPIC_CONFIG_KEY, "__topology");
        kafkaProperties.put(KafkaPublisher.METRIC_TOPIC_CONFIG_KEY, "__topology__metrics");

        //publish metrics every 2 seconds
        kafkaProperties.setProperty(TopologyClient.PUBLISH_INTERVAL_CONFIG_KEY, "2000");

        //set the topic Lenses listens for topology information
        kafkaProperties.setProperty(KafkaPublisher.METRIC_TOPIC_CONFIG_KEY, "__topology__metrics");

        //set the topic Lenses listens for metrics information
        kafkaProperties.setProperty(KafkaPublisher.TOPOLOGY_TOPIC_CONFIG_KEY, "__topology");

        SuspiciousPaymentsService suspiciousPaymentsService = new SuspiciousPaymentsService(suspiciousPayments);
        suspiciousPaymentsService.run(kafkaProperties);

        Thread.sleep(Long.MAX_VALUE);
    }
}