package com.landoop.lenses.topology.example.kstreams;

import com.landoop.lenses.topology.client.*;
import com.landoop.lenses.topology.client.kafka.metrics.KafkaPublisher;
import com.landoop.lenses.topology.client.kafka.metrics.KafkaTopologyClient;
import com.landoop.lenses.topology.client.kafka.metrics.TopologyKafkaStreamsClientSupplier;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;

public class App {

  private static final String inputTopic = "wordcount-input";
  private static final String outputTopic = "wordcount-output-kstreams";

  public static void main(final String[] args) throws Exception {
    if (args.length != 1 || args[0].isEmpty()) {
      throw new IllegalArgumentException("args[0] should provider the Kafka Brokers");
    }

    final String brokers = args[0];
    Topology topology = TopologyBuilder.start(AppType.KafkaStreams, "kafka-streams-wordcount")
        .withTopic(inputTopic)
        .withRepresentation(Representation.TABLE)
        .endNode()
        .withNode("groupby", NodeType.SELECT)
        .withDescription("Group by word")
        .withRepresentation(Representation.TABLE)
        .withParent(inputTopic)
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
    topologyProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    topologyProps.put(KafkaPublisher.TOPOLOGY_TOPIC_CONFIG_KEY, "__topology");
    topologyProps.put(KafkaPublisher.METRIC_TOPIC_CONFIG_KEY, "__topology__metrics");
    TopologyClient client = KafkaTopologyClient.create(topologyProps);
    client.register(topology);

    // this regular expression will split up the lines on whitespace
    final Pattern pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);

    final Properties streamProps = new Properties();

    // name our application
    streamProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-app");
    streamProps.put(StreamsConfig.CLIENT_ID_CONFIG, "wordcount-app-client");

    // run against a local kafka cluster, set up via docker
    streamProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);

    // Setup default serdes for keys and values
    streamProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    streamProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

    // In our sample application we will flush records every 5 seconds
    streamProps.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 5000);

    final StreamsBuilder builder = new StreamsBuilder();

    // Construct a `KStream` from the input topic "streams-plaintext-input", where message values
    // represent lines of text (for the sake of this example, we ignore whatever may be stored
    // in the message keys).
    //
    // Note: We could also just call `builder.stream("streams-plaintext-input")` if we wanted to leverage
    // the default serdes specified in the Streams configuration above, because these defaults
    // match what's in the actual topic.  However we explicitly set the deserializers in the
    // call to `stream()` below in order to show how that's done, too.
    final KStream<String, String> stream = builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()));

    final KTable<String, Long> wordCounts = stream
        // Split each text line, by whitespace, into words.  The text lines are the record
        // values, i.e. we can ignore whatever data is in the record keys and thus invoke
        // `flatMapValues()` instead of the more generic `flatMap()`.
        .flatMapValues(value -> Arrays.asList(pattern.split(value.toLowerCase())))
        // Count the occurrences of each word (record key).
        //
        // This will change the stream type from `KStream<String, String>` to `KTable<String, Long>`
        // (word -> count).
        //
        .groupBy((key, word) -> word)
        .count();

    // take the output table and write it to an output topic
    wordCounts.toStream().to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

    // Now that we have finished the definition of the processing topology we can actually run
    // it via `start()`.  The Streams application as a whole can be launched just like any
    // normal Java application that has a `main()` method.
    final KafkaStreams streams = new KafkaStreams(builder.build(), new StreamsConfig(streamProps), new TopologyKafkaStreamsClientSupplier(client, topology));

    streams.cleanUp();
    streams.start();

    produceInputData(brokers);
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