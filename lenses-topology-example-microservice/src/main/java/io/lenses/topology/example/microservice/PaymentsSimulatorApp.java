package io.lenses.topology.example.microservice;

import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class PaymentsSimulatorApp {

  private static final String paymentsTopic = "payments";
  private static final String convertedPaymentsTopic = "payments_xchg";
  private static final String suspiciousPayments = "suspicious_payments";

  public static void main(final String[] args) throws Exception {
    if (args.length != 1 || args[0].isEmpty()) {
      throw new IllegalArgumentException("args[0] should provider the Kafka Brokers");
    }

    final String brokers = args[0];

    final CurrencyExchangeRepo exchangeRepo = new CurrencyExchangeRepo();
    PaymentsSimulator simulator = new PaymentsSimulator(exchangeRepo);

    final Properties kafkaProperties = new Properties();
    kafkaProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    //run the simulator
    simulator.run(kafkaProperties, paymentsTopic);

    Thread.sleep(Long.MAX_VALUE);
  }
}