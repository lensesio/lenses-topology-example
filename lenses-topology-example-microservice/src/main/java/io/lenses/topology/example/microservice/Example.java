package io.lenses.topology.example.microservice;

import com.landoop.lenses.topology.client.TopologyClient;
import com.landoop.lenses.topology.client.kafka.metrics.KafkaPublisher;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class Example {

  private static final String inputTopic = "transactions";
  private static final String outputTopic1 = "transaction_converted";
  private static final String outputTopic2 = "transaction_converted_high";

  public static void main(final String[] args) throws Exception {
    if (args.length != 1 || args[0].isEmpty()) {
      throw new IllegalArgumentException("args[0] should provider the Kafka Brokers");
    }

    final String brokers = args[0];

    final CurrencyExchangeRepo exchangeRepo = new CurrencyExchangeRepo();
    TransactionsSimulator simulator = new TransactionsSimulator(exchangeRepo);

    final Properties kafkaProperties = new Properties();
    kafkaProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    //run the simulator
    simulator.run(kafkaProperties, inputTopic);

    //publish metrics every 2 seconds
    kafkaProperties.setProperty(TopologyClient.PUBLISH_INTERVAL_CONFIG_KEY, "2000");

    //set the topic Lenses listens for topology information
    kafkaProperties.setProperty(KafkaPublisher.METRIC_TOPIC_CONFIG_KEY, "__topology__metrics_tre");

    //set the topic Lenses listens for metrics information
    kafkaProperties.setProperty(KafkaPublisher.TOPOLOGY_TOPIC_CONFIG_KEY, "__topology_tre");

    MyMicroserviceApp app = new MyMicroserviceApp(exchangeRepo, inputTopic, outputTopic1, outputTopic2);
    app.run(kafkaProperties);

    Thread.sleep(Long.MAX_VALUE);
  }
}