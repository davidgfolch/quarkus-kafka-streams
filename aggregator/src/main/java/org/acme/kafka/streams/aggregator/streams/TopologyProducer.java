package org.acme.kafka.streams.aggregator.streams;

import org.acme.kafka.streams.aggregator.model.Aggregation;
import org.acme.kafka.streams.aggregator.model.Temperature;
import org.acme.kafka.streams.aggregator.model.WeatherStation;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import java.time.Instant;

import static java.lang.Double.parseDouble;

@ApplicationScoped
public class TopologyProducer {

    @Produces
    public Topology buildTopology() {
        StreamsBuilder sb = new StreamsBuilder();
        sb.stream(Temperature.TOPIC, Consumed.with(Serdes.Integer(), Serdes.String()))
                .join(sb.globalTable(WeatherStation.TOPIC, WeatherStation.CONSUMER),
                        (stationId, timestampAndValue) -> stationId, this::stationTemperatureValueJoiner)
                .groupByKey()
                .aggregate(Aggregation::new, (stationId, value, aggregation) -> aggregation.updateFrom(value),
                        Aggregation.MATERIALIZED)
                .toStream()
                .to(Aggregation.TOPIC, Produced.with(Serdes.Integer(), Aggregation.SERDE));
        return sb.build();
    }

    private Temperature stationTemperatureValueJoiner(String timestampAndValue, WeatherStation ws) {
        String[] parts = timestampAndValue.split(";");
        return new Temperature(ws.id, ws.name, Instant.parse(parts[0]), parseDouble(parts[1]));
    }
}
