package org.acme.kafka.streams.aggregator.model;

import io.quarkus.kafka.client.serialization.ObjectMapperSerde;
import io.quarkus.runtime.annotations.RegisterForReflection;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Consumed;

@RegisterForReflection
public class WeatherStation {

    public static final String TOPIC = "weather-stations";
    public static final Consumed<Integer, WeatherStation> CONSUMER = Consumed.with(Serdes.Integer(), new ObjectMapperSerde<>(WeatherStation.class));

    public int id;
    public String name;

    @SuppressWarnings("unused") //needed for reflection?
    public WeatherStation(){
    }

    public WeatherStation(int id, String name){
        this.id = id;
        this.name = name;
    }
}
