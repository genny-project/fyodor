package life.genny.fyodor.endpoints;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.persistence.EntityManager;

import org.jboss.logging.Logger;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.rest.client.inject.RestClient;

import io.quarkus.runtime.StartupEvent;
import io.quarkus.runtime.ShutdownEvent;

import io.vertx.core.http.HttpServerRequest;

import life.genny.fyodor.utils.SearchUtility;
import life.genny.fyodor.utils.CacheUtils;
import life.genny.qwandaq.entity.BaseEntity;
import life.genny.qwandaq.entity.SearchEntity;
import life.genny.qwandaq.message.QSearchBeResult;
import life.genny.qwandaq.models.GennyToken;
import life.genny.qwandaq.utils.KeycloakUtils;
import life.genny.fyodor.service.ApiService;

/**
 * SearchEndpoint - Endpoints providing classic Genny Search functionality
 */

@Path("/")
@ApplicationScoped
public class SearchEndpoint {

	private static final Logger log = Logger.getLogger(SearchEndpoint.class);

	@ConfigProperty(name = "genny.keycloak.url", defaultValue = "https://keycloak.gada.io")
	String baseKeycloakUrl;

	@ConfigProperty(name = "project.version", defaultValue = "unknown")
	String version;

	@Context
	HttpServerRequest request;

	@Inject
	@RestClient
	ApiService apiService;

	@Inject
	EntityManager entityManager;

	@Inject
	CacheUtils cacheUtils;

	@Inject
	SearchUtility search;

	/**
	* A GET request for the running fyodor version
	*
	* @return 	version data
	 */
	@GET
	@Path("/api/version")
	public Response version() {
		return Response.ok().entity("version: \""+version+"\"").build();
	}

	/**
	* A GET request for a specific baseentity
	* 
	* @param code		The BaseEntity Code
	* @return			The BaseEntity
	 */
	@GET
	@Path("/api/entity/{code}")
	public Response entity(String code) {

		BaseEntity entity = search.fetchBaseEntityFromDB(code);

		if (entity != null) {
			return Response.ok().entity(entity).build();
		}
		return Response.status(Response.Status.NOT_FOUND).build();
	}

	/**
	 * 
	 * A POST request for search results based on a SearchEntity
	 *
	 * @return Success
	 */
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/api/search")
	public Response search(SearchEntity searchEntity) {
		log.info("Search POST received..");
		GennyToken userToken = null;

		String token = null;
		try {
			token = request.getHeader("authorization").split("Bearer ")[1];
			if (token != null) {
				userToken = new GennyToken(token);
			} else {
				log.error("Bad token in Search GET provided");
				return Response.ok().build();
			}
		} catch (Exception e) {
			log.error("Bad or no header token in Search POST provided");
			return Response.ok().build();
		}

		// Process search
		QSearchBeResult results = search.findBySearch25(searchEntity, false, false);

		return Response.ok().entity(results).build();
	}
}
