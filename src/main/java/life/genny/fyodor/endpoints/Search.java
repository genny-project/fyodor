package life.genny.fyodor.endpoints;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.json.bind.Jsonb;
import javax.json.bind.JsonbBuilder;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.persistence.EntityManager;

import org.jboss.logging.Logger;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import io.vertx.core.http.HttpServerRequest;

import life.genny.fyodor.utils.SearchUtility;
import life.genny.qwandaq.entity.SearchEntity;
import life.genny.qwandaq.message.QSearchBeResult;
import life.genny.qwandaq.models.GennyToken;

/**
 * Search - Endpoints providing classic Genny Search functionality
 */

@Path("/")
@ApplicationScoped
public class Search {

	private static final Logger log = Logger.getLogger(Search.class);

	@ConfigProperty(name = "genny.keycloak.url", defaultValue = "https://keycloak.gada.io")
	String baseKeycloakUrl;

	@Context
	HttpServerRequest request;

	@Inject
	EntityManager entityManager;

	@Inject
	SearchUtility search;

	Jsonb jsonb = JsonbBuilder.create();

	/**
	 * 
	 * A POST request for search results based on a 
	 * {@link SearchEntity}. Will only fetch codes.
	 *
	 * @return Success
	 */
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/api/search")
	public Response search(SearchEntity searchEntity) {

		log.info("Search POST received..");
		String token = null;

		try {
			token = request.getHeader("authorization").split("Bearer ")[1];
			if (token != null) {
				GennyToken userToken = new GennyToken(token);
			} else {
				log.error("Bad token in Search GET provided");
				return Response.status(Response.Status.FORBIDDEN).build();
			}
		} catch (Exception e) {
			log.error("Bad or no header token in Search POST provided");
			return Response.status(Response.Status.BAD_REQUEST).build();
		}

		// Process search
		QSearchBeResult results = search.findBySearch25(searchEntity, false, false);
		log.info("Found " + results.getTotal() + " results!");

		String json = jsonb.toJson(results);
		return Response.ok().entity(json).build();
	}

	/**
	 * 
	 * A POST request for search results based on a 
	 * {@link SearchEntity}. Will fetch complete entities.
	 *
	 * @return Success
	 */
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/api/search/fetch")
	public Response fetch(SearchEntity searchEntity) {

		log.info("Fetch POST received..");
		String token = null;

		try {
			token = request.getHeader("authorization").split("Bearer ")[1];
			if (token != null) {
				GennyToken userToken = new GennyToken(token);
			} else {
				log.error("Bad token in Search GET provided");
				return Response.status(Response.Status.FORBIDDEN).build();
			}
		} catch (Exception e) {
			log.error("Bad or no header token in Search POST provided");
			return Response.status(Response.Status.BAD_REQUEST).build();
		}

		// Process search
		QSearchBeResult results = search.findBySearch25(searchEntity, false, true);
		log.info("Found " + results.getTotal() + " results!");

		String json = jsonb.toJson(results);
		return Response.ok().entity(json).build();
	}
}
