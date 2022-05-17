package com.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.Properties;

public class SimpleKafkaProcessor {
    private static String APPLICATION_NAME = "processor-application";
    private static String BOOTSTRAP_SERVERS = "localhost:9092";
    private static String STREAM_LOG = "stream_log";
    private static String STREAM_LOG_FILTER = "stream_log_copy_filter";

    private static String SOURCE = "Source";
    private static String PROCESS = "Process";
    private static String SINK = "Sink";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        Topology topology = new Topology();
        topology.addSource(SOURCE, STREAM_LOG)
                // addProcessor, addSink의 세 번째 파라미터는 부모 노드이다.
                .addProcessor(PROCESS, () -> new FilterProcessor(), SOURCE)
                .addSink(SINK, STREAM_LOG_FILTER, PROCESS);

        KafkaStreams streaming = new KafkaStreams(topology, props);
        streaming.start();
    }
}
