package com.landoop.lenses.topology.example.akkastreams;

import akka.actor.ActorSystem;
import akka.japi.function.Function;
import akka.kafka.ConsumerSettings;
import akka.kafka.ProducerSettings;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Consumer;
import akka.kafka.javadsl.Producer;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Keep;
import com.landoop.lenses.topology.client.AppType;
import com.landoop.lenses.topology.client.NodeType;
import com.landoop.lenses.topology.client.Representation;
import com.landoop.lenses.topology.client.Topology;
import com.landoop.lenses.topology.client.TopologyBuilder;
import com.landoop.lenses.topology.client.TopologyClient;
import com.landoop.lenses.topology.client.akka.streams.AkkaStreamsKafkaMetricBuilder;
import com.landoop.lenses.topology.client.kafka.metrics.KafkaMetricsBuilder;
import com.landoop.lenses.topology.client.kafka.metrics.KafkaTopologyClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Arrays;
import java.util.Properties;

public class App {

    private static final String inputTopic = "wordcount-input";
    private static final String outputTopic = "wordcount-output-akkastreams";

    public static void main(final String[] args) throws Exception {

        Topology topology = TopologyBuilder.start(AppType.AkkaStreams, "akka-streams-wordcount")
                .withTopic(inputTopic)
                .withRepresentation(Representation.TABLE)
                .endNode()
                .withNode("groupby", NodeType.GROUPBY)
                .withDescription("Group by word")
                .withRepresentation(Representation.TABLE)
                .withParent(inputTopic)
                .endNode()
                .withNode("count", NodeType.COUNT)
                .withDescription("Count words")
                .withRepresentation(Representation.TABLE)
                .withParent("groupby")
                .endNode()
                .withTopic(outputTopic)
                .withParent("count")
                .withRepresentation(Representation.TABLE)
                .endNode()
                .build();

        Properties topologyProps = new Properties();
        topologyProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        TopologyClient client = KafkaTopologyClient.create(topologyProps);
        client.register(topology);

        ActorSystem system = ActorSystem.create();
        ActorMaterializer materializer = ActorMaterializer.create(system);

        final ConsumerSettings<String, String> consumerSettings =
                ConsumerSettings.create(system, new StringDeserializer(), new StringDeserializer())
                        .withBootstrapServers("localhost:9092")
                        .withGroupId("topology-client-worker");

        final ProducerSettings<String, String> producerSettings =
                ProducerSettings
                        .create(system, new StringSerializer(), new StringSerializer())
                        .withBootstrapServers("localhost:9092");

        KafkaProducer<String, String> producer = producerSettings.createKafkaProducer();

        Consumer.Control control = Consumer.plainSource(consumerSettings, Subscriptions.topics(inputTopic))
                .mapConcat((Function<ConsumerRecord<String, String>, Iterable<String>>) param -> {
                    String line = param.value();
                    return Arrays.asList(line.split(" "));
                })
                .map(value -> new ProducerRecord<String, String>(outputTopic, value))
                .toMat(Producer.plainSink(producerSettings, producer), Keep.left())
                .run(materializer);

        client.register(topology.getAppName(), inputTopic, new AkkaStreamsKafkaMetricBuilder(control));
        client.register(topology.getAppName(), outputTopic, new KafkaMetricsBuilder(producer));

        produceInputData();
    }

    private static void produceInputData() throws InterruptedException {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
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