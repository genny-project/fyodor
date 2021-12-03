package life.genny.fyodor.live.data;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import javax.json.bind.Jsonb;
import javax.json.bind.JsonbBuilder;
import javax.json.bind.JsonbConfig;

import org.jboss.logging.Logger;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.rest.client.inject.RestClient;

import io.quarkus.runtime.StartupEvent;
import io.quarkus.runtime.ShutdownEvent;
import io.smallrye.reactive.messaging.annotations.Blocking;

import life.genny.fyodor.models.GennyToken;
import life.genny.fyodor.utils.SearchUtility;
import life.genny.fyodor.utils.KeycloakUtils;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import life.genny.qwandaq.entity.SearchEntity;
import life.genny.qwandaq.entity.BaseEntity;
import life.genny.qwandaq.message.QSearchMessage;
import life.genny.qwandaq.message.QSearchBeResult;
import life.genny.qwandaq.message.QDataBaseEntityMessage;
import life.genny.qwandaq.attribute.EntityAttribute;

import javax.persistence.EntityManager;
import life.genny.fyodor.service.ApiService;

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

	@ConfigProperty(name = "quarkus.datasource.jdbc.url")
	String uri;

	@Inject
	InternalProducer producer;

	@Inject
	@RestClient
	ApiService apiService;

	@Inject
	EntityManager entityManager;

	SearchUtility search;

	GennyToken serviceToken;

    void onStart(@Observes StartupEvent ev) {
        log.info("The Consumer is starting...");

		log.info("SQL URL = " + uri);

		// Initialise token, search util and attribute map
		serviceToken = new KeycloakUtils().getToken(baseKeycloakUrl, keycloakRealm, clientId, secret, serviceUsername, servicePassword, null);
		search = new SearchUtility(serviceToken, entityManager, apiService);
		search.loadAllAttributesIntoCache(serviceToken);

        log.info("Finished Consumer Setup!");
    }

    void onStop(@Observes ShutdownEvent ev) {
        log.info("The Consumer is stopping...");
    }

	@Incoming("search_events")
	@Blocking
	public void getSearchEvents(String data) {
		log.info("Received incoming Search Event... ");
		log,info(data);

		// Deserialize with null values to avoid deserialisation errors
		JsonbConfig config = new JsonbConfig();
		Jsonb jsonb = JsonbBuilder.create(config);
		QSearchMessage msg = jsonb.fromJson(data, QSearchMessage.class);
		GennyToken userToken = new GennyToken(msg.getToken());
		SearchEntity searchBE = msg.getSearchEntity();

		if (searchBE == null) {
			log.error("Message did NOT contain a SearchEntity!!!");
			return;
		}

		// Process search
		QSearchBeResult results = search.processSearchEntity(searchBE);

		// Perform count for any combined search attributes
		Long totalResultCount = 0L;
		for (EntityAttribute ea : searchBE.getBaseEntityAttributes()) {
			if (ea.getAttributeCode().startsWith("CMB_")) {
				String combinedSearchCode = ea.getAttributeCode().substring("CMB_".length());
				SearchEntity combinedSearch = (SearchEntity) search.fetchBaseEntityFromCache(combinedSearchCode, serviceToken);
				Long subTotal = search.performCount(combinedSearch);
				if (subTotal != null) {
					totalResultCount += subTotal;
				} else {
					log.info("subTotal count for " + combinedSearchCode + " is NULL");
				}
			}
		}

		// TODO: Sort out this damn Nested Search

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

			String json = jsonb.toJson(results);
			producer.getToSearchData().send(json);

		} else if (msg.getDestination().equals("webcmds")) {

			QDataBaseEntityMessage entityMsg = new QDataBaseEntityMessage(results.getEntities());
			entityMsg.setTotal(results.getTotal());
			entityMsg.setReplace(true);
			entityMsg.setParentCode(searchBE.getCode());
			entityMsg.setToken(userToken.getToken());
			String json = jsonb.toJson(entityMsg);
			producer.getToWebCmds().send(json);

			QDataBaseEntityMessage searchBEMsg = new QDataBaseEntityMessage(searchBE);
			searchBEMsg.setToken(userToken.getToken());
			String searchJson = jsonb.toJson(searchBEMsg);
			producer.getToWebCmds().send(searchJson);

		}
	}
}
