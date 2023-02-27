package org.acme.kafka.streams.aggregator.streams;

import org.acme.kafka.streams.aggregator.model.Aggregation;
import org.acme.kafka.streams.aggregator.model.WeatherStationData;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.jboss.logging.Logger;
import org.wildfly.common.net.HostName;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.List;

import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.acme.kafka.streams.aggregator.Constants.STORE_WEATHER_STATIONS;

@ApplicationScoped
public class InteractiveQueries {

    private static final Logger LOG = Logger.getLogger(InteractiveQueries.class);

    @Inject
    KafkaStreams streams;

    public List<PipelineMetadata> getMetaData() {
        return streams.streamsMetadataForStore(STORE_WEATHER_STATIONS).stream().map(sm ->
                new PipelineMetadata(
                        sm.hostInfo().host() + ":" + sm.hostInfo().port(),
                        sm.topicPartitions().stream().map(TopicPartition::toString).collect(toSet()))
        ).collect(toList());
    }

    public GetWeatherStationDataResult getWeatherStationData(int id) {
        KeyQueryMetadata metadata = streams.queryMetadataForKey(STORE_WEATHER_STATIONS, id, Serdes.Integer().serializer());
        if (metadata == null || metadata == KeyQueryMetadata.NOT_AVAILABLE) {
            LOG.warnv("Found no metadata for key {0}", id);
            return GetWeatherStationDataResult.NOT_FOUND;
        }
        final var activeHost = metadata.activeHost();
        if (activeHost.host().equals(HostName.getQualifiedHostName())) {
            LOG.infov("Found data for key {0} locally", id);
            return ofNullable(getWeatherStationStore().get(id))
                    .map(ag-> GetWeatherStationDataResult.found(new WeatherStationData(ag)))
                    .orElse(GetWeatherStationDataResult.NOT_FOUND);
        }
        LOG.infov("Found data for key {0} on remote host {1}:{2}", id, activeHost.host(), activeHost.port());
        return GetWeatherStationDataResult.foundRemotely(activeHost.host(), activeHost.port());
    }

    private ReadOnlyKeyValueStore<Integer, Aggregation> getWeatherStationStore() {
        while (true) {
            try {
                return streams.store(StoreQueryParameters.fromNameAndType(STORE_WEATHER_STATIONS, QueryableStoreTypes.keyValueStore()));
            } catch (InvalidStateStoreException e) {
                // ignore, store not ready yet
            }
        }
    }
}
