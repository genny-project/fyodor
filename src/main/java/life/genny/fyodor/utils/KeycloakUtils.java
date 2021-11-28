package life.genny.fyodor.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.json.JsonObject;
import javax.json.bind.Jsonb;
import javax.json.bind.JsonbBuilder;
import javax.net.ssl.HttpsURLConnection;
import javax.persistence.EntityManager;

import org.apache.commons.lang3.StringUtils;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import life.genny.fyodor.models.GennyToken;

public class KeycloakUtils {

	private static final Logger log = Logger.getLogger(KeycloakUtils.class);

	@Inject
	EntityManager entityManager;

	Jsonb jsonb = JsonbBuilder.create();

	public KeycloakUtils() {};

	public GennyToken getToken(String keycloakUrl, String realm, String clientId, String secret, String username,
			String password, String refreshToken) {

		HashMap<String, String> postDataParams = new HashMap<>();
		// postDataParams.put("Content-Type", "application/x-www-form-urlencoded");

		if (refreshToken == null) {
			postDataParams.put("username", username);
			postDataParams.put("password", password);
			log.debug("using username " + username);
			log.debug("using password " + password);
			log.debug("using client_id " + clientId);
			log.debug("using client_secret " + secret);
			postDataParams.put("grant_type", "password");
		} else {
			postDataParams.put("refresh_token", refreshToken);
			postDataParams.put("grant_type", "refresh_token");
			log.debug("using refresh token");
			log.debug(refreshToken);
		}

		postDataParams.put("client_id", clientId);
		if (!StringUtils.isBlank(secret)) {
			postDataParams.put("client_secret", secret);
		}

		String requestURL = keycloakUrl + "/auth/realms/" + realm + "/protocol/openid-connect/token";

		String postDataStr = getPostDataString(postDataParams);
		// String str =keycloakService.getAccessToken(realm, postDataStr);
		String str = performPostCall(requestURL, postDataParams);

		log.debug("keycloak auth url = " + requestURL);
		log.debug(username + " token= " + str);

		JsonObject json = jsonb.fromJson(str, JsonObject.class);
		String accessToken = json.getString("access_token");
		GennyToken token = new GennyToken(accessToken);
		return token;
	}

	public String performPostCall(String requestURL, HashMap<String, String> postDataParams) {

		URL url;
		String response = "";
		try {
			url = new URL(requestURL);

			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setReadTimeout(15000);
			conn.setConnectTimeout(15000);
			conn.setRequestMethod("POST");
			conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
			conn.setDoInput(true);
			conn.setDoOutput(true);

			OutputStream os = conn.getOutputStream();
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
			writer.write(getPostDataString(postDataParams));

			writer.flush();
			writer.close();
			os.close();

			String line;
			BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
			while ((line = br.readLine()) != null) {
				response += line;
			}
			if (conn.getResponseCode() == HttpsURLConnection.HTTP_OK) {
				log.info("Successful Token Request!");
			} else {
				log.error("Bad Token Request: " + conn.getResponseCode() + " " + conn.getResponseMessage());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return response;
	}

	private String getPostDataString(HashMap<String, String> params) {
		StringBuilder result = new StringBuilder();
		boolean first = true;

		try {
			for (Map.Entry<String, String> entry : params.entrySet()) {
				log.debug("key: " + entry.getKey() + ", value: " + entry.getValue());
				if (first)
					first = false;
				else
					result.append("&");

				result.append(URLEncoder.encode(entry.getKey(), "UTF-8"));
				result.append("=");
				result.append(URLEncoder.encode(entry.getValue(), "UTF-8"));
			}
		} catch (UnsupportedEncodingException e) {
			log.error("Error encoding Post Data String: " + e.getStackTrace());
		}

		return result.toString();
	}

}
