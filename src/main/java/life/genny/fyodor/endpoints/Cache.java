package life.genny.fyodor.endpoints;

import javax.inject.Inject;
import javax.json.bind.Jsonb;
import javax.json.bind.JsonbBuilder;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.jboss.logging.Logger;
import org.jboss.resteasy.annotations.jaxrs.PathParam;

import io.vertx.core.http.HttpServerRequest;
import life.genny.qwandaq.utils.CacheUtils;
import life.genny.qwandaq.utils.HttpUtils;
import life.genny.qwandaq.utils.SecurityUtils;
import life.genny.serviceq.Service;

/**
 * Cache --- Endpoints providing cache access
 *
 * @author jasper.robison@gada.io
 *
 */
@Path("/cache")
public class Cache {

	private static final Logger log = Logger.getLogger(Cache.class);

	static Jsonb jsonb = JsonbBuilder.create();

	@Context
	HttpServerRequest request;

	@Inject
	Service service;

	/**
	* Read an item from the cache.
	*
	* @param key The key of the cache item
	* @return The json item
	 */
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/{key}")
	public Response read(@HeaderParam("Authorization") String token, @PathParam("key") String key) {

		Boolean authorized = SecurityUtils.isAuthorizedToken(token);
		if (!authorized) {
			return Response.status(Response.Status.BAD_REQUEST)
				.entity(HttpUtils.error("Not authorized to make this request")).build();
		}

		String realm = service.getServiceToken().getRealm();
		String json = (String) CacheUtils.readCache(realm, key);

		if (json == null) {
			return Response.ok("null").build();
		}

		log.info("Found json of length " + json.length() + " for " + key);

		return Response.ok(json).build();
	}

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/{key}")
	public Response write(@HeaderParam("Authorization") String token, @PathParam("key") String key, String value) {

		Boolean authorized = SecurityUtils.isAuthorizedToken(token);
		if (!authorized) {
			return Response.status(Response.Status.BAD_REQUEST)
				.entity(HttpUtils.error("Not authorized to make this request")).build();
		}

		String realm = service.getServiceToken().getRealm();
		CacheUtils.writeCache(realm, key, value);

		log.info("Wrote json of length " + value.length() + " for " + key);

		return Response.ok().build();
	}

	@DELETE
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/{key}")
	public Response remove(@HeaderParam("Authorization") String token, @PathParam("key") String key) {

		Boolean authorized = SecurityUtils.isAuthorizedToken(token);
		if (!authorized) {
			return Response.status(Response.Status.BAD_REQUEST)
				.entity(HttpUtils.error("Not authorized to make this request")).build();
		}

		String realm = service.getServiceToken().getRealm();
		CacheUtils.removeEntry(realm, key);

		log.info("Removed Item for " + key);

		return Response.ok().build();
	}

}
