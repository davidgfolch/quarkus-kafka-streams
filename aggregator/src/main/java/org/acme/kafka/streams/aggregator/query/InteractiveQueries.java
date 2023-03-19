package org.acme.kafka.streams.aggregator.query;

import org.acme.kafka.streams.aggregator.model.WeatherStationTemperature;
import org.acme.kafka.streams.aggregator.query.domain.GetWeatherStationDataResult;
import org.acme.kafka.streams.aggregator.query.domain.PipelineMetadata;
import org.acme.kafka.streams.aggregator.query.domain.WeatherStationData;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.eclipse.microprofile.faulttolerance.Retry;
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

    //todo this makes AggregatorTest fail
    //todo decouple api from aggregator kafka streams
//    public void onStart(@Observes final StartupEvent startupEvent) {
//        streams.start();
//    }
//
//    public void onStop(@Observes final ShutdownEvent shutdownEvent) {
//        streams.close();
//    }

    public List<PipelineMetadata> getMetaData() {
        return streams.streamsMetadataForStore(STORE_WEATHER_STATIONS).stream().map(sm ->
                new PipelineMetadata(sm.hostInfo().host() + ":" + sm.hostInfo().port(),
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
                    .map(WeatherStationData::new).map(GetWeatherStationDataResult::found)
                    .orElse(GetWeatherStationDataResult.NOT_FOUND);
        }
        LOG.infov("Found data for key {0} on remote host {1}:{2}", id, activeHost.host(), activeHost.port());
        return GetWeatherStationDataResult.foundRemotely(activeHost.host(), activeHost.port());
    }

    @Retry(maxRetries = Integer.MAX_VALUE, retryOn = InvalidStateStoreException.class) // ignore, store not ready yet
    private ReadOnlyKeyValueStore<Integer, WeatherStationTemperature> getWeatherStationStore() {
        return streams.store(StoreQueryParameters.fromNameAndType(STORE_WEATHER_STATIONS, QueryableStoreTypes.keyValueStore()));
    }
}
