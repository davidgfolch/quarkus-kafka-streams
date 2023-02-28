package org.acme.kafka.streams.aggregator.api;

import org.acme.kafka.streams.aggregator.query.domain.GetWeatherStationDataResult;
import org.acme.kafka.streams.aggregator.query.InteractiveQueries;
import org.acme.kafka.streams.aggregator.query.domain.PipelineMetadata;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.OptionalInt;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;

@Produces(APPLICATION_JSON)
@ApplicationScoped
@Path("/weather-stations")
public class WeatherStationEndpoint {

    @Inject
    InteractiveQueries interactiveQueries;

    @ConfigProperty(name = "quarkus.http.ssl-port")
    int sslPort;

    @GET
    @Path("/data/{id}")
    public Response getWeatherStationData(int id) {
        GetWeatherStationDataResult res = interactiveQueries.getWeatherStationData(id);
        if (res.getResult().isPresent()) {
            return Response.ok(res.getResult().get()).build();
        }
        if (res.getHost().isPresent()) {
            return Response.seeOther(getOtherUri(res.getHost().get(), res.getPort(), id)).build();
        }
        return Response.status(NOT_FOUND.getStatusCode(), "No data found for weather station " + id).build();
    }

    @GET
    @Path("/meta-data")
    public List<PipelineMetadata> getMetaData() {
        return interactiveQueries.getMetaData();
    }

    private URI getOtherUri(String host, OptionalInt port, int id) {
        try {
            String scheme = (port.isPresent() && port.getAsInt() == sslPort) ? "https" : "http";
            return new URI(scheme + "://" + host + ":" + port + "/weather-stations/data/" + id);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
