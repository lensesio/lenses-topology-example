package io.lenses.topology.example.microservice;

import io.lenses.topology.client.TopologyClient;
import io.lenses.topology.client.kafka.metrics.MicroserviceTopology;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class SuspiciousPaymentsService implements AutoCloseable {
  private final String suspiciousPaymentsTopic;
  private final ExecutorService threadPool = Executors.newFixedThreadPool(1);
  private volatile boolean stop = false;

  public SuspiciousPaymentsService(String suspiciousPaymentsTopic) {
    this.suspiciousPaymentsTopic = suspiciousPaymentsTopic;
  }

  private static KafkaProducer<String, String> createProducer(Properties properties) {
    Properties props = new Properties();
    props.putAll(properties);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    return new KafkaProducer<>(props);
  }
  public void run(Properties properties) {
    threadPool.submit(() -> {
      try {
        try (KafkaConsumer<String, String> consumer = createConsumer(properties)) {
          consumer.subscribe(Collections.singleton(suspiciousPaymentsTopic));
          try (TopologyClient topologyClient = createTopology(properties, consumer)) {
            while (!stop) {
              consumer.poll(Duration.ofSeconds(1)).forEach(record -> {
                //code to handle suspicious transactions
              });
            }
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      } catch (Throwable throwable) {
        System.out.println(throwable);
      }
    });
  }


  private TopologyClient createTopology(Properties properties,
                                        KafkaConsumer<String, String> consumer) throws IOException {
    return MicroserviceTopology.fromConsumer("SuspiciousPaymentsService", consumer, Collections.singletonList(suspiciousPaymentsTopic), properties);
  }

  private static KafkaConsumer<String, String> createConsumer(Properties properties) {
    Properties props = new Properties();
    props.putAll(properties);
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "50000");
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "SuspiciousPaymentsService");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    return new KafkaConsumer<>(props);
  }

  @Override
  public void close() throws Exception {
    stop = true;
    threadPool.awaitTermination(2, TimeUnit.SECONDS);
  }
}
