package org.acme.kafka.streams.aggregator.query.domain;

import java.util.Optional;
import java.util.OptionalInt;

import static java.util.Optional.ofNullable;

public class GetWeatherStationDataResult {

    public static final GetWeatherStationDataResult NOT_FOUND = new GetWeatherStationDataResult(null, null, null);

    private final WeatherStationData result;
    private final String host;
    private final Integer port;

    public GetWeatherStationDataResult(WeatherStationData result, String host, Integer port) {
        this.result = result;
        this.host = host;
        this.port = port;
    }

    public static GetWeatherStationDataResult found(WeatherStationData data) {
        return new GetWeatherStationDataResult(data, null, null);
    }

    public static GetWeatherStationDataResult foundRemotely(String host, int port) {
        return new GetWeatherStationDataResult(null, host, port);
    }

    public Optional<WeatherStationData> getResult() {
        return ofNullable(result);
    }

    public Optional<String> getHost() {
        return ofNullable(host);
    }

    public OptionalInt getPort() {
        return ofNullable(port).map(OptionalInt::of).orElse(OptionalInt.empty());
    }
}
