package org.acme.kafka.streams.aggregator.model;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;

public class BaseDomain {

    public static <K, V> GlobalKTable<K, V> globalTable(StreamsBuilder sb, String topic, Consumed<K, V> consumed) {
        return sb.globalTable(topic, consumed);
    }

}
