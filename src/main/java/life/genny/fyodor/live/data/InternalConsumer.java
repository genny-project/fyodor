package life.genny.fyodor.live.data;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import javax.json.bind.Jsonb;
import javax.json.bind.JsonbBuilder;
import javax.json.bind.JsonbConfig;
import javax.json.JsonObject;
import javax.persistence.EntityManager;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.jboss.logging.Logger;

import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.reactive.messaging.annotations.Blocking;
import life.genny.fyodor.service.ApiService;
import life.genny.fyodor.utils.SearchUtility;
import life.genny.fyodor.utils.CacheUtils;

import life.genny.qwandaq.attribute.Attribute;
import life.genny.qwandaq.attribute.EntityAttribute;
import life.genny.qwandaq.entity.SearchEntity;
import life.genny.qwandaq.exception.BadDataException;
import life.genny.qwandaq.message.QDataBaseEntityMessage;
import life.genny.qwandaq.message.QSearchBeResult;
import life.genny.qwandaq.message.QSearchMessage;
import life.genny.qwandaq.message.QBulkMessage;
import life.genny.qwandaq.models.GennyToken;
import life.genny.qwandaq.utils.KeycloakUtils;

@ApplicationScoped
public class InternalConsumer {

	private static final Logger log = Logger.getLogger(InternalConsumer.class);

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
	InternalProducer producer;

	@Inject
	@RestClient
	ApiService apiService;

	@Inject
	SearchUtility search;

	GennyToken serviceToken;

	@Inject
	CacheUtils cacheUtils;

    void onStart(@Observes StartupEvent ev) {
		serviceToken = new KeycloakUtils().getToken(baseKeycloakUrl, keycloakRealm, clientId, secret, serviceUsername, servicePassword, null);
    }

	@Incoming("search_events")
	@Blocking
	public void getSearchEvents(String data) {
		log.info("Received incoming Search Event... ");
		log.debug(data);

		Instant start = Instant.now();

		// Deserialize with null values to avoid deserialisation errors
		JsonbConfig config = new JsonbConfig();
		Jsonb jsonb = JsonbBuilder.create(config);
		QSearchMessage msg = jsonb.fromJson(data, QSearchMessage.class);
		GennyToken userToken = new GennyToken(msg.getToken());
		SearchEntity searchBE = msg.getSearchEntity();
		log.info("Token: " + msg.getToken());

		if (searchBE == null) {
			log.error("Message did NOT contain a SearchEntity!!!");
			return;
		}

		log.info("Handling search " + searchBE.getCode());

        // Check if a filter is being used
        final String templateSearchCode  = searchBE.getCode().replaceFirst("_"+userToken.getSessionCode().toUpperCase(), "");
        
        QBulkMessage bulkMsg = null;
        Boolean noCachePresent = true;
        Boolean usingCache = searchBE.is("SCH_CACHABLE");
        Integer pageStart = -1;
        
        String wildcard = searchBE.getValueAsString("SCH_WILDCARD");
        if (wildcard != null && !wildcard.isBlank()) {
        	usingCache = false;
        }
        
		usingCache = false;
		noCachePresent = true;

		if (usingCache) {

			pageStart = searchBE.getValue("SCH_PAGE_START",0);
			log.info("Fetching Table Search from Cache with pageStart = "+pageStart);

			if (pageStart == 0) {
				// only do caching if the searchsession matches the original
				String jsonData = apiService.getDataFromCache(serviceToken.getRealm(), "SPEEDUP:"+templateSearchCode, "Bearer " + serviceToken.getToken());
				JsonObject json = jsonb.fromJson(jsonData, JsonObject.class);

				if ("ok".equalsIgnoreCase(json.getString("status"))) {
					String value = json.getString("value");
					bulkMsg = jsonb.fromJson(value, QBulkMessage.class);
				}

				if (bulkMsg == null) {
					noCachePresent = true;

				} else {

					if (bulkMsg.getMessages()[0].getParentCode()!=null) {
						bulkMsg.getMessages()[0].setParentCode(templateSearchCode+"_"+userToken.getSessionCode().toUpperCase());
					} else {
						Arrays.stream(bulkMsg.getMessages()[0].getItems()).forEach(i -> i.setCode(templateSearchCode+"_"+userToken.getSessionCode().toUpperCase()));        				  
					}

					if (bulkMsg.getMessages().length>0) {
						if (bulkMsg.getMessages()[1].getParentCode()!=null) {
							bulkMsg.getMessages()[1].setParentCode(templateSearchCode+"_"+userToken.getSessionCode().toUpperCase());
						} else {
							Arrays.stream(bulkMsg.getMessages()[1].getItems()).forEach(i -> i.setCode(templateSearchCode+"_"+userToken.getSessionCode().toUpperCase()));        				  
						}
					}
					// update with latest user token
					bulkMsg.setToken(userToken.getToken());
					noCachePresent = false;
				}
			}
		}

		if (noCachePresent) {
			// Process search
			bulkMsg = search.processSearchEntity(searchBE, userToken);

			if ((pageStart == 0) && usingCache) {
				String json = jsonb.toJson(bulkMsg);
				apiService.writeDataIntoCache("SPEEDUP:"+templateSearchCode, json, "Bearer " + userToken.getToken());
			}
		} 

		Instant end = Instant.now();
		log.info("Finished " + (usingCache ? "WITH" : "WITHOUT") + " caching - Duration: " + Duration.between(start, end).toMillis() + " millSeconds.");

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

		// Publish results to destination channel
		if (msg.getDestination().equals("search_data")) {

			String json = jsonb.toJson(bulkMsg);
			producer.getToSearchData().send(json);

		} else if (msg.getDestination().equals("webcmds")) {

			String json = jsonb.toJson(bulkMsg);
			producer.getToWebCmds().send(json);

		}
	}
}
