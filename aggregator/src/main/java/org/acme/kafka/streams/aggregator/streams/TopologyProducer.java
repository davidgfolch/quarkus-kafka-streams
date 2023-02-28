package org.acme.kafka.streams.aggregator.streams;

import org.acme.kafka.streams.aggregator.model.Aggregation;
import org.acme.kafka.streams.aggregator.model.Temperature;
import org.acme.kafka.streams.aggregator.model.WeatherStation;
import org.apache.kafka.streams.Topology;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

import static org.acme.kafka.streams.aggregator.streams.BaseKafka.buildKafkaStream;

@ApplicationScoped
public class TopologyProducer {

    @Produces
    public Topology buildTopology() {
        return buildKafkaStream(sb ->
                sb.stream(Temperature.TOPIC, Temperature.CONSUMED).join(WeatherStation.getGlobalTable(sb),
                        (stationId, temperature) -> stationId, Temperature::join
                ).groupByKey()
                        .aggregate(Aggregation::new,
                                (stationId, value, aggregation) -> aggregation.updateFrom(value),
                                Aggregation.MATERIALIZED)
                        .toStream()
                        .to(Aggregation.TOPIC, Aggregation.PRODUCED));
    }

}
