package life.genny.fyodor.live.data;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import javax.json.JsonObject;
import javax.json.bind.Jsonb;
import javax.json.bind.JsonbBuilder;
import javax.json.bind.JsonbConfig;
import javax.persistence.EntityManager;

import java.time.Duration;
import java.time.Instant;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.jboss.logging.Logger;

import io.quarkus.runtime.StartupEvent;
import io.smallrye.reactive.messaging.annotations.Blocking;
import life.genny.fyodor.intf.KafkaBean;
import life.genny.fyodor.service.ApiService;
import life.genny.fyodor.utils.SearchUtility;

import life.genny.qwandaq.data.GennyCache;
import life.genny.qwandaq.entity.SearchEntity;
import life.genny.qwandaq.message.QSearchMessage;
import life.genny.qwandaq.message.QBulkMessage;
import life.genny.qwandaq.models.GennyToken;
import life.genny.qwandaq.utils.BaseEntityUtils;
import life.genny.qwandaq.utils.CacheUtils;
import life.genny.qwandaq.utils.DatabaseUtils;
import life.genny.qwandaq.utils.KafkaUtils;
import life.genny.qwandaq.utils.KeycloakUtils;
import life.genny.qwandaq.utils.QwandaUtils;
import life.genny.qwandaq.data.BridgeSwitch;

@ApplicationScoped
public class InternalConsumer {

	static final Logger log = Logger.getLogger(InternalConsumer.class);

    static Jsonb jsonb = JsonbBuilder.create();

	@ConfigProperty(name = "genny.keycloak.url", defaultValue = "https://keycloak.gada.io")
	String baseKeycloakUrl;

	@ConfigProperty(name = "genny.keycloak.realm", defaultValue = "genny")
	String keycloakRealm;

	@ConfigProperty(name = "genny.service.username", defaultValue = "service")
	String serviceUsername;

	@ConfigProperty(name = "genny.service.password", defaultValue = "password")
	String servicePassword;

	@ConfigProperty(name = "genny.oidc.client-id", defaultValue = "backend")
	String clientId;

	@ConfigProperty(name = "genny.oidc.credentials.secret", defaultValue = "secret")
	String secret;

	@Inject
	EntityManager entityManager;

	@Inject
	InternalProducer producer;

	@Inject
	@RestClient
	ApiService apiService;

	@Inject
	SearchUtility search;

	@Inject 
	GennyCache cache;

	@Inject
	KafkaBean kafkaBean;

	GennyToken serviceToken;

	BaseEntityUtils beUtils;

    void onStart(@Observes StartupEvent ev) {

		serviceToken = KeycloakUtils.getToken(baseKeycloakUrl, keycloakRealm, clientId, secret, serviceUsername, servicePassword);

		// Init Utility Objects
		beUtils = new BaseEntityUtils(serviceToken);

		// Establish connection to DB and cache, and init utilities
		DatabaseUtils.init(entityManager);
		CacheUtils.init(cache);
		KafkaUtils.init(kafkaBean);
		QwandaUtils.init(serviceToken);

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
		boolean replace = msg.getReplace();

		// update bridge switch
		// String jti = userToken.getUniqueId();
		// String bridgeId = msg.getBridgeId();
		// BridgeSwitch.bridges.put(jti, bridgeId);

		SearchEntity searchBE = msg.getSearchEntity();
		log.info("Token: " + msg.getToken());

		if (searchBE == null) {
			log.error("Message did NOT contain a SearchEntity!!!");
			return;
		}

		log.info("Handling search " + searchBE.getCode());


        QBulkMessage bulkMsg = search.processSearchEntity(searchBE, userToken, replace);

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
		String json = jsonb.toJson(bulkMsg);
		KafkaUtils.writeMsg(msg.getDestination(), json);
	}
}
