package io.lenses.topology.example.spark.kafka;

import com.landoop.lenses.topology.client.*;
import com.landoop.lenses.topology.client.kafka.metrics.KafkaTopologyClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

public class App {

  private static String inputTopic = "wordcount-input";
  private static String outputTopic = "wordcount-output-spark";

  public static void main(String[] args) throws StreamingQueryException, IOException, InterruptedException {
    if (args.length != 1 || args[0].isEmpty()) {
      throw new IllegalArgumentException("args[0] should provider the Kafka Brokers");
    }
    String brokers = args[0];

    Topology topology = TopologyBuilder.start(AppType.SparkStreaming, "spark-streaming-wordcount")
        .withTopic(inputTopic)
        .withDescription("Raw lines of text")
        .withRepresentation(Representation.TABLE)
        .endNode()
        .withNode("groupby", NodeType.GROUPBY)
        .withDescription("Group by value")
        .withRepresentation(Representation.TABLE)
        .withParent("wordcount-input")
        .endNode()
        .withNode("count", NodeType.COUNT)
        .withDescription("Count value")
        .withRepresentation(Representation.TABLE)
        .withParent("groupby")
        .endNode()
        .withTopic(outputTopic)
        .withParent("count")
        .withDescription("Words put onto the output")
        .withRepresentation(Representation.TABLE)
        .endNode()
        .build();

    Properties topologyProps = new Properties();
    topologyProps.setProperty("lenses.topics.topology", "__topology");
    topologyProps.setProperty("lenses.topics.metrics", "__topology__metrics");

    topologyProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    TopologyClient client = KafkaTopologyClient.create(topologyProps);
    client.register(topology);

    SparkSession spark = SparkSession
        .builder()
        .master("local[4]")
        .appName("kafka-topology")
            .config("lenses.topics.topology", "__topology")
            .config("lenses.topics.metrics", "__topology__metrics")
        .getOrCreate();

    Dataset<Row> words = spark
        .readStream()
        .format("lenses-kafka")
        .option("kafka.bootstrap.servers", brokers)
        .option("kafka.lenses.topology.description", topology.getDescription())
        .option("lenses.topics.topology", "__topology")
        .option("lenses.topics.metrics", "__topology__metrics")
        .option("subscribe", inputTopic)
        .load();

    Dataset<Row> wordCounts = words.selectExpr("CAST(value AS STRING)").flatMap(
        (FlatMapFunction<Row, String>) row -> Arrays.stream(row.getAs("value").toString().split(" ")).iterator(),
        Encoders.STRING()
    ).groupBy("value").count();

    StreamingQuery query = wordCounts.writeStream()
        .format("kafka")
        .outputMode(OutputMode.Update())
        .option("checkpointLocation", "/tmp/checkpoint")
        .option("kafka.bootstrap.servers", brokers)
            .option("lenses.topics.topology", "__topology")
            .option("lenses.topics.metrics", "__topology__metrics")

            .option("topic", outputTopic)
        .start();

    produceInputData(brokers);
    query.awaitTermination();
  }

  private static void produceInputData(String brokers) throws InterruptedException {

    Properties props = new Properties();
    props.put("bootstrap.servers", brokers);
    props.put("key.serializer", StringSerializer.class.getName());
    props.put("value.serializer", StringSerializer.class.getName());
    KafkaProducer<String, String> producer = new KafkaProducer<>(props);

    String[] lines = new String[]{
        "I can't. As much as I care about you, my first duty is to the ship.",
        "Captain, why are we out here chasing comets?",
        "The Federation's gone; the Borg is everywhere!",
        "This is not about revenge.",
        "This is about justice.",
        "I'd like to think that I haven't changed those things, sir.",
        "The game's not big enough unless it scares you a little.",
        "Congratulations - you just destroyed the Enterprise.",
        "The look in your eyes, I recognize it.",
        "You used to have it for me.",
        "How long can two people talk about nothing?",
        "I guess it's better to be lucky than good.",
        "But the probability of making a six is no greater than that of rolling a seven.",
        "We finished our first sensor sweep of the neutral zone.",
        "Wait a minute - you've been declared dead."
    };

    while (true) {
      for (String line : lines) {
        producer.send(new ProducerRecord<>(inputTopic, line));
      }
      Thread.sleep(1000);
    }
  }
}