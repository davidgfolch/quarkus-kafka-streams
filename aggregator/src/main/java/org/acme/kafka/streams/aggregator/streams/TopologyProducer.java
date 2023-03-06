package org.acme.kafka.streams.aggregator.streams;

import org.acme.kafka.streams.aggregator.model.Temperature;
import org.acme.kafka.streams.aggregator.model.WeatherStation;
import org.acme.kafka.streams.aggregator.model.WeatherStationTemperature;
import org.apache.kafka.streams.Topology;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

@ApplicationScoped
public class TopologyProducer {

    @Produces
    public Topology build() {
        return BaseKafka.build(sb ->
                sb.stream(Temperature.TOPIC, Temperature.CONSUMED)
                        .join(WeatherStation.getGlobalTable(sb), (stationId, temperature) -> stationId, Temperature::join)
                        .groupByKey()
                        .aggregate(WeatherStationTemperature::new,
                                (stationId, value, a) -> a.updateFrom(value), WeatherStationTemperature.MATERIALIZED)
                        .toStream()
                        .to(WeatherStationTemperature.TOPIC, WeatherStationTemperature.PRODUCED));
    }

}
