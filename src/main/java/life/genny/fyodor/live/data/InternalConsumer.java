package life.genny.fyodor.live.data;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import javax.json.bind.Jsonb;
import javax.json.bind.JsonbBuilder;

import java.time.Duration;
import java.time.Instant;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.jboss.logging.Logger;

import io.quarkus.runtime.StartupEvent;
import io.smallrye.reactive.messaging.annotations.Blocking;
import life.genny.fyodor.utils.SearchUtility;

import life.genny.qwandaq.entity.SearchEntity;
import life.genny.qwandaq.message.QSearchMessage;
import life.genny.qwandaq.message.QBulkMessage;
import life.genny.qwandaq.models.GennyToken;
import life.genny.qwandaq.utils.KafkaUtils;
import life.genny.serviceq.Service;

@ApplicationScoped
public class InternalConsumer {

	static final Logger log = Logger.getLogger(InternalConsumer.class);

    static Jsonb jsonb = JsonbBuilder.create();

	@Inject
	Service service;

	@Inject
	SearchUtility search;

    void onStart(@Observes StartupEvent ev) {

		service.showConfiguration();

		service.initToken();
		service.initDatabase();
		service.initCache();
		service.initAttributes();
		service.initKafka();
		log.info("[*] Finished Startup!");
    }

	@Incoming("search_events")
	@Blocking
	public void getSearchEvents(String data) {
		log.info("Received incoming Search Event... ");
		log.debug(data);

		Instant start = Instant.now();

		// Deserialize with null values to avoid deserialisation errors
		QSearchMessage msg = jsonb.fromJson(data, QSearchMessage.class);
		GennyToken userToken = new GennyToken(msg.getToken());

		SearchEntity searchBE = msg.getSearchEntity();
		log.info("Token: " + msg.getToken());

		if (searchBE == null) {
			log.error("Message did NOT contain a SearchEntity!!!");
			return;
		}

		log.info("Handling search " + searchBE.getCode());

        QBulkMessage bulkMsg = search.processSearchEntity(searchBE, userToken);

		Instant end = Instant.now();
		log.info("Finished! - Duration: " + Duration.between(start, end).toMillis() + " millSeconds.");

		// TODO: Sort out this Nested Search

		// // Perform Nested Searches
		// List<EntityAttribute> nestedSearches = searchBE.findPrefixEntityAttributes("SBE_");

		// for (EntityAttribute search : nestedSearches) {
		// 	String[] fields = search.getAttributeCode().split("\\.");

		// 	if (fields == null || fields.length < 2) {
		// 		continue;
		// 	}

		// 	for (BaseEntity target : msg.getItems()) {
		// 		searchTable(beUtils, fields[0], true, fields[1], target.getCode());
		// 	}
		// }

		// check for null destination
		if (msg.getDestination() == null) {
			log.error("Destination is null! Not Sending results.");
			return;
		}

		// publish results to destination channel
		KafkaUtils.writeMsg(msg.getDestination(), bulkMsg);
	}
}
