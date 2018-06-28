package com.landoop.lenses.topology.example.spark.kafka;

import com.landoop.lenses.topology.client.NodeType;
import com.landoop.lenses.topology.client.Representation;
import com.landoop.lenses.topology.client.Topology;
import com.landoop.lenses.topology.client.TopologyBuilder;
import com.landoop.lenses.topology.client.TopologyClient;
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

    public static void main(String[] args) throws StreamingQueryException, IOException {

        String inputTopic = "wordcount-input";

        Topology topology = TopologyBuilder.start("my app")
                .withTopic(inputTopic)
                .withDescription("Raw lines of text")
                .withRepresentation(Representation.TABLE)
                .finish()
                .withNode("groupby", NodeType.GROUPBY)
                .withDescription("Group by value")
                .withRepresentation(Representation.TABLE)
                .withParent("wordcount-input")
                .finish()
                .withNode("count", NodeType.COUNT)
                .withDescription("Count value")
                .withRepresentation(Representation.TABLE)
                .withParent("groupby")
                .finish()
                .withNode("console", NodeType.TABLE)
                .withParent("count")
                .withDescription("Words put onto the output")
                .withRepresentation(Representation.TABLE)
                .finish()
                .build();

        new Thread(() -> {
            Properties props = new Properties();
            props.put("bootstrap.servers", "PLAINTEXT://localhost:9092");
            props.put("key.serializer", StringSerializer.class);
            props.put("value.serializer", StringSerializer.class);
            KafkaProducer<String, String> producer = new KafkaProducer<>(props);
            while (true) {
                try {
                    Thread.sleep(30);
                    producer.send(new ProducerRecord<>(inputTopic, "hello world"));
                    producer.flush();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();

        Properties topologyProps = new Properties();
        topologyProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        TopologyClient client = KafkaTopologyClient.create(topologyProps);
        client.register(topology);

        SparkSession spark = SparkSession
                .builder()
                .master("local[4]")
                .appName("kafka-topology")
                .getOrCreate();

        Dataset<Row> words = spark
                .readStream()
                .format("lenses-kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("kafka.lenses.topology.description", topology.getDescription())
                .option("subscribe", inputTopic)
                .load();

        Dataset<Row> wordCounts = words.selectExpr("CAST(value AS STRING)").flatMap(
                (FlatMapFunction<Row, String>) row -> Arrays.stream(row.getAs("value").toString().split(" ")).iterator(),
                Encoders.STRING()
        ).groupBy("value").count();

        StreamingQuery query = wordCounts.writeStream()
                .outputMode(OutputMode.Complete())
                .format("console")
                .start();

        query.awaitTermination();
    }
}