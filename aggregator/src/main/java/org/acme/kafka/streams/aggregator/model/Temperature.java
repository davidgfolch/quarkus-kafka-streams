package org.acme.kafka.streams.aggregator.model;

import java.time.Instant;

public class Temperature {

    public static final String TOPIC = "temperature-values";

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
}
