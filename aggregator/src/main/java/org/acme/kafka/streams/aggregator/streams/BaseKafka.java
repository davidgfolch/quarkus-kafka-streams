package org.acme.kafka.streams.aggregator.streams;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;

import java.util.function.Consumer;

public class BaseKafka {

    public static Topology build(Consumer<StreamsBuilder> fnc) {
        StreamsBuilder sb = new StreamsBuilder();
        fnc.accept(sb);
        return sb.build();
    }
}
