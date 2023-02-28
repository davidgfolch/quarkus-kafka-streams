package org.acme.kafka.streams.aggregator.streams;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;

import java.util.function.Consumer;

public class BaseKafka {

    public static <K, V> GlobalKTable<K, V> globalTable(StreamsBuilder sb, String topic, Consumed<K, V> consumed) {
        return sb.globalTable(topic, consumed);
    }

    public static Topology buildKafkaStream(Consumer<StreamsBuilder> fnc) {
        StreamsBuilder sb = new StreamsBuilder();
        fnc.accept(sb);
        return sb.build();
    }
}
