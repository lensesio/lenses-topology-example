package io.lenses.topology.example.microservice;

import io.lenses.topology.client.TopologyClient;
import io.lenses.topology.client.kafka.metrics.KafkaPublisher;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class S3StorageServiceApp {

  private static final String paymentsTopic = "payments";
  private static final String convertedPaymentsTopic = "payments_xchg";
  private static final String suspiciousPayments = "suspicious_payments";

  public static void main(final String[] args) throws Exception {
    if (args.length != 1 || args[0].isEmpty()) {
      throw new IllegalArgumentException("args[0] should provider the Kafka Brokers");
    }

    final String brokers = args[0];

    final Properties kafkaProperties = new Properties();
    kafkaProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);

    //publish metrics every 2 seconds
    kafkaProperties.setProperty(TopologyClient.PUBLISH_INTERVAL_CONFIG_KEY, "2000");

    //set the topic Lenses listens for topology information
    kafkaProperties.put(KafkaPublisher.METRIC_TOPIC_CONFIG_KEY, "__topology__metrics");

    //set the topic Lenses listens for metrics information
    kafkaProperties.put(KafkaPublisher.TOPOLOGY_TOPIC_CONFIG_KEY, "__topology");

    S3StorageService service = new S3StorageService(convertedPaymentsTopic);
    service.run(kafkaProperties);

    Thread.sleep(Long.MAX_VALUE);
  }
}