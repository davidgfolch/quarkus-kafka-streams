package org.acme.kafka.streams.aggregator.model;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Consumed;

import java.time.Instant;

import static java.lang.Double.parseDouble;

public class Temperature {

    public static final String TOPIC = "temperature-values";
    public static final Consumed<Integer,String> CONSUMED = Consumed.with(Serdes.Integer(), Serdes.String());
    public static final String SEP = ";";

    public int stationId;
    public String stationName;
    public Instant timestamp;
    public double value;

    public Temperature(int stationId, String stationName, Instant timestamp, double value) {
        this.stationId = stationId;
        this.stationName = stationName;
        this.timestamp = timestamp;
        this.value = value;
    }

    public static Temperature join(String timestampAndValue, WeatherStation ws) {
        String[] parts = timestampAndValue.split(SEP);
        return new Temperature(ws.id, ws.name, Instant.parse(parts[0]), parseDouble(parts[1]));
    }
}
