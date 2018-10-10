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

public class MyMicroserviceApp implements AutoCloseable {
  private final CurrencyExchangeRepo repo;
  private final String transactionsTopic;
  private final String convertedTransactionTopic;
  private final String highAmountTransactionsTopic;
  private final ExecutorService threadPool = Executors.newFixedThreadPool(1);
  private volatile boolean stop = false;

  public MyMicroserviceApp(CurrencyExchangeRepo repo,
                           String transactionsTopic,
                           String convertedTransactionTopic,
                           String highAmountTransactionsTopic) {
    this.repo = repo;
    this.transactionsTopic = transactionsTopic;
    this.convertedTransactionTopic = convertedTransactionTopic;
    this.highAmountTransactionsTopic = highAmountTransactionsTopic;
  }

  public void run(Properties properties) {
    threadPool.submit(() -> {
      try {
        try (KafkaConsumer<String, String> consumer = createConsumer(properties)) {
          consumer.subscribe(Collections.singleton(transactionsTopic));
          try (KafkaProducer<String, String> producer = createProducer(properties)) {
            try (KafkaProducer<String, String> highTransactionsProducer = createProducer(properties)) {
              try (TopologyClient topologyClient = createTopology(properties, consumer, producer, highTransactionsProducer)) {
                while (!stop) {
                  consumer.poll(Duration.ofSeconds(1)).forEach(record -> {
                    try {
                      final Transaction transaction = JacksonSupport.mapper.readValue(record.value(), Transaction.class);
                      processTransaction(transaction, producer, highTransactionsProducer);
                    } catch (IOException e) {
                      e.printStackTrace();
                    }
                  });
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
                                        KafkaProducer<String, String> highTransactionsProducer) throws IOException {

    Map<KafkaProducer<?, ?>, List<String>> producersMap = new HashMap<>();
    producersMap.put(producer, Collections.singletonList(convertedTransactionTopic));
    producersMap.put(highTransactionsProducer, Collections.singletonList(highAmountTransactionsTopic));

    Map<KafkaConsumer<?, ?>, List<String>> consumersMap = Collections.singletonMap(consumer, Collections.singletonList(transactionsTopic));

    return MicroserviceTopology.create("TransactionsService", producersMap, consumersMap, properties);
  }

  private void processTransaction(final Transaction transaction,
                                  KafkaProducer<String, String> producer,
                                  KafkaProducer<String, String> highTransactionsProducer) throws JsonProcessingException {
    final Transaction exchange = applyExchangeRate(transaction);
    final String value = JacksonSupport.mapper.writeValueAsString(exchange);
    producer.send(new ProducerRecord<>(convertedTransactionTopic, exchange.getCurrency(), value));
    if (exchange.getAmount().compareTo(new BigDecimal(10000)) > 0) {
      highTransactionsProducer.send(new ProducerRecord<>(highAmountTransactionsTopic, exchange.getCurrency(), value));
    }
  }

  private Transaction applyExchangeRate(final Transaction transaction) {
    BigDecimal rate = repo.getExchangeRate(transaction.getCurrency());
    return new Transaction(transaction.getCurrency(), rate.multiply(transaction.getAmount()), transaction.getTimestamp());
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
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "TransactionServices");
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
