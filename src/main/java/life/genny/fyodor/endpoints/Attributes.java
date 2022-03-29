package life.genny.fyodor.endpoints;

import javax.inject.Inject;
import javax.json.bind.Jsonb;
import javax.json.bind.JsonbBuilder;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.jboss.logging.Logger;
import org.jboss.resteasy.annotations.jaxrs.PathParam;

import io.vertx.core.http.HttpServerRequest;
import life.genny.qwandaq.attribute.Attribute;
import life.genny.qwandaq.utils.DatabaseUtils;
import life.genny.serviceq.Service;

/**
 * Attribute --- Endpoints providing database attribute access
 *
 * @author jasper.robison@gada.io
 *
 */
@Path("/attribute")
public class Attributes {

	private static final Logger log = Logger.getLogger(Attributes.class);

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
	@Path("/{code}")
	public Response read(@PathParam String code) {

		String realm = service.getServiceToken().getRealm();
		Attribute attribute = DatabaseUtils.findAttributeByCode(realm, code);

		return Response.ok(attribute).build();
	}
}
