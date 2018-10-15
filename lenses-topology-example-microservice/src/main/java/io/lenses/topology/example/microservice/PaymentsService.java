package io.lenses.topology.example.microservice;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.landoop.lenses.topology.client.JacksonSupport;
import com.landoop.lenses.topology.client.TopologyClient;
import com.landoop.lenses.topology.client.kafka.metrics.MicroserviceTopology;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class PaymentsService implements AutoCloseable {
  private final CurrencyExchangeRepo repo;
  private final String PaymentsTopic;
  private final String convertedPaymentsTopic;
  private final String suspiciousPaymentsTopic;
  private final ExecutorService threadPool = Executors.newFixedThreadPool(1);
  private volatile boolean stop = false;

  public PaymentsService(CurrencyExchangeRepo repo,
                         String PaymentsTopic,
                         String convertedPaymentsTopic,
                         String suspiciousPaymentsTopic) {
    this.repo = repo;
    this.PaymentsTopic = PaymentsTopic;
    this.convertedPaymentsTopic = convertedPaymentsTopic;
    this.suspiciousPaymentsTopic = suspiciousPaymentsTopic;
  }

  public void run(Properties properties) {
    threadPool.submit(() -> {
      try {
        try (KafkaConsumer<String, String> consumer = createConsumer(properties)) {
          consumer.subscribe(Collections.singleton(PaymentsTopic));
          try (KafkaProducer<String, String> producer = createProducer(properties)) {
            try (KafkaProducer<String, String> suspicisousPaymentsProducer = createProducer(properties)) {
              try (TopologyClient topologyClient = createTopology(properties, consumer, producer, suspicisousPaymentsProducer)) {
                while (!stop) {
                  consumer.poll(Duration.ofSeconds(1)).forEach(record -> {
                    try {
                      final Payment payment = JacksonSupport.mapper.readValue(record.value(), Payment.class);
                      processPayment(payment, producer, suspicisousPaymentsProducer);
                    } catch (IOException e) {
                      e.printStackTrace();
                    }
                  });
                  producer.flush();
                  suspicisousPaymentsProducer.flush();
                }
              } catch (IOException e) {
                e.printStackTrace();
              }
            }
          }
        }
      } catch (Throwable throwable) {
        System.out.println(throwable);
      }
    });
  }


  private TopologyClient createTopology(Properties properties,
                                        KafkaConsumer<String, String> consumer,
                                        KafkaProducer<String, String> producer,
                                        KafkaProducer<String, String> suspiciousPaymentsProducer) throws IOException {

    Map<KafkaProducer<?, ?>, List<String>> producersMap = new HashMap<>();
    producersMap.put(producer, Collections.singletonList(convertedPaymentsTopic));
    producersMap.put(suspiciousPaymentsProducer, Collections.singletonList(suspiciousPaymentsTopic));

    Map<KafkaConsumer<?, ?>, List<String>> consumersMap = Collections.singletonMap(consumer, Collections.singletonList(PaymentsTopic));

    return MicroserviceTopology.create("PaymentsService", producersMap, consumersMap, properties);
  }

  private void processPayment(final Payment Payment,
                              KafkaProducer<String, String> producer,
                              KafkaProducer<String, String> suspiciousEntriesProducer) throws JsonProcessingException {
    final Payment exchange = applyExchangeRate(Payment);
    final String value = JacksonSupport.mapper.writeValueAsString(exchange);
    producer.send(new ProducerRecord<>(convertedPaymentsTopic, exchange.getCurrency(), value));
    //any Payment smaller than 10 pounds  (for the sake of the demo)
    if (exchange.getAmount().compareTo(new BigDecimal(10000)) < 0) {
      suspiciousEntriesProducer.send(new ProducerRecord<>(suspiciousPaymentsTopic, exchange.getCurrency(), value));
    }
  }

  private Payment applyExchangeRate(final Payment Payment) {
    BigDecimal rate = repo.getExchangeRate(Payment.getCurrency());
    return new Payment(Payment.getCurrency(), rate.multiply(Payment.getAmount()), Payment.getTimestamp());
  }

  private static KafkaProducer<String, String> createProducer(Properties properties) {
    Properties props = new Properties();
    props.putAll(properties);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    return new KafkaProducer<>(props);
  }

  private static KafkaConsumer<String, String> createConsumer(Properties properties) {
    Properties props = new Properties();
    props.putAll(properties);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "PaymentsServices");
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "50000");
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
