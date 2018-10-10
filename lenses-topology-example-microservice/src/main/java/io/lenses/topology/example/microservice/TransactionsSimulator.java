package io.lenses.topology.example.microservice;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.landoop.lenses.topology.client.JacksonSupport;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.math.BigDecimal;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class TransactionsSimulator implements AutoCloseable {
  private final CurrencyExchangeRepo repo;
  private final ExecutorService threadPool = Executors.newFixedThreadPool(1);
  private volatile boolean stop = false;

  public TransactionsSimulator(CurrencyExchangeRepo repo) {
    this.repo = repo;
  }

  public void run(Properties properties, String topic) {
    threadPool.submit(() -> {
      try (KafkaProducer<String, String> producer = createProducer(properties)) {
        while (!stop) {
          repo.getCurrencies().forEach(currency -> {
            final Transaction transaction = generateTransaction(currency);
            try {
              producer.send(new ProducerRecord<>(topic, currency, JacksonSupport.mapper.writeValueAsString(transaction)));
            } catch (JsonProcessingException e) {
              System.out.println("An error occurred while simulating transactions. " + e.getMessage());
            }
          });

          producer.flush();
          Thread.sleep(400);
        }
      } catch (Throwable throwable) {
        System.out.println("An error occurred while simulating transactions. " + throwable.getMessage());
      }
      threadPool.shutdown();
    });
  }

  private static KafkaProducer<String, String> createProducer(Properties properties) {
    Properties props = new Properties();
    props.putAll(properties);
    props.put("key.serializer", StringSerializer.class.getName());
    props.put("value.serializer", StringSerializer.class.getName());
    return new KafkaProducer<>(props);
  }

  private Transaction generateTransaction(String currency) {
    double random = ThreadLocalRandom.current().nextDouble(1, 2000000);
    return new Transaction(currency, new BigDecimal(Math.abs(random)), System.currentTimeMillis());
  }

  @Override
  public void close() throws Exception {
    stop = true;
    threadPool.awaitTermination(2, TimeUnit.SECONDS);
  }
}
