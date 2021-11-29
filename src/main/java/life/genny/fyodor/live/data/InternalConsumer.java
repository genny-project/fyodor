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
import io.smallrye.reactive.messaging.annotations.Blocking;
import io.quarkus.runtime.ShutdownEvent;

import life.genny.fyodor.models.GennyToken;
import life.genny.fyodor.utils.SearchUtility;
import life.genny.fyodor.utils.KeycloakUtils;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import life.genny.qwandaq.entity.SearchEntity;
import life.genny.qwandaq.message.QSearchBeResult;
import life.genny.qwandaq.message.QSearchMessage;

import javax.persistence.EntityManager;
import life.genny.fyodor.service.ApiService;
import life.genny.fyodor.live.data.InternalProducer;


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
	EntityManager entityManager;

	SearchUtility search;

	GennyToken serviceToken;

    void onStart(@Observes StartupEvent ev) {
        log.info("The application is starting...");

		// Initialise token, search util and attribute map
		serviceToken = new KeycloakUtils().getToken(baseKeycloakUrl, keycloakRealm, clientId, secret, serviceUsername, servicePassword, null);
		search = new SearchUtility(serviceToken, entityManager, apiService);
		search.loadAllAttributesIntoCache(serviceToken);

        log.info("Finished Setup!");
    }

    void onStop(@Observes ShutdownEvent ev) {
        log.info("The application is stopping...");
    }

	@Incoming("search_events")
	@Blocking
	public void getSearchEvents(String data) {
		log.info("Received incoming Search Event... ");
		// Deserialize with null values to avoid deserialisation errors
		JsonbConfig config = new JsonbConfig().withNullValues(true);
		Jsonb jsonb = JsonbBuilder.create(config);
		QSearchMessage msg = jsonb.fromJson(data, QSearchMessage.class);
		SearchEntity searchBE = msg.getSearchEntity();
		// Process our messsage
		QSearchBeResult results = search.findBySearch25(searchBE, false, true);
		log.info("results = " + results.getTotal().toString());

		// Publish results to data channel
		String json = jsonb.toJson(msg);
		producer.getToSearchData().send(json);
	}

}
