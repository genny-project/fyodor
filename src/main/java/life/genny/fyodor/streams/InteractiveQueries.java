package life.genny.fyodor.streams;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import life.genny.qwandaq.attribute.Attribute;

@ApplicationScoped
public class InteractiveQueries {

	static String ATTRIBUTE_STORE = "replication-attribute";

    @Inject
    KafkaStreams streams;

    public Attribute getAttribute(String code) {
        return getAttributeStore().get(code);
    }

    private ReadOnlyKeyValueStore<String, Attribute> getAttributeStore() {
        while (true) {
            try {
                return streams.store(StoreQueryParameters.fromNameAndType(ATTRIBUTE_STORE, QueryableStoreTypes.keyValueStore()));
            } catch (InvalidStateStoreException e) {
                // ignore, store not ready yet
            }
        }
    }
}
