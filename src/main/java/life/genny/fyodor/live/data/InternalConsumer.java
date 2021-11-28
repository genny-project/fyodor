package life.genny.fyodor.live.data;

import javax.enterprise.event.Observes;
import javax.inject.Inject;

import org.jboss.logging.Logger;
import org.eclipse.microprofile.reactive.messaging.Incoming;

import io.quarkus.runtime.StartupEvent;
import io.quarkus.runtime.ShutdownEvent;

import life.genny.fyodor.models.GennyToken;
import life.genny.fyodor.utils.SearchUtility;
import life.genny.fyodor.utils.KeycloakUtils;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.persistence.EntityManager;

public class InternalConsumer {

	private static final Logger log = Logger.getLogger(InternalConsumer.class);

	SearchUtility search;

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

    void onStart(@Observes StartupEvent ev) {
        log.info("The application is starting...");

		GennyToken serviceToken = new KeycloakUtils().getToken(baseKeycloakUrl, keycloakRealm, clientId, secret, serviceUsername, servicePassword, null);
		search = new SearchUtility(serviceToken, entityManager);
		search.loadAllAttributesIntoCache(serviceToken);
    }

    void onStop(@Observes ShutdownEvent ev) {
        log.info("The application is stopping...");
    }

	@Incoming("search_events")
	public void getSearchEvents(String data) {
		log.info("Received incoming Search Event... " + data);
		search.processSearchEvent(data);
	}

}
