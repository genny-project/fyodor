package life.genny.fyodor.intf;

import org.jboss.logging.Logger;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.json.JsonObject;
import javax.json.bind.Jsonb;
import javax.json.bind.JsonbBuilder;

import io.smallrye.reactive.messaging.kafka.OutgoingKafkaRecordMetadata;
import org.eclipse.microprofile.reactive.messaging.Message;

import life.genny.qwandaq.data.BridgeSwitch;
import life.genny.qwandaq.intf.KafkaInterface;
import life.genny.qwandaq.models.GennyToken;
import life.genny.fyodor.live.data.InternalProducer;

@ApplicationScoped
public class KafkaBean implements KafkaInterface {

	private static final Logger log = Logger.getLogger(KafkaBean.class);

    static Jsonb jsonb = JsonbBuilder.create();

	@Inject 
	InternalProducer producer;

	/**
	* Write a string payload to a kafka channel.
	*
	* @param channel
	* @param payload
	 */
	public void write(String channel, String payload) { 

		// JsonObject event = jsonb.fromJson(payload, JsonObject.class);
		// GennyToken userToken = new GennyToken(event.getString("token"));

		if ("webcmds".equals(channel)) {
			producer.getToWebCmds().send(payload);

			// String bridgeId = BridgeSwitch.bridges.get(userToken.getUniqueId());

			// OutgoingKafkaRecordMetadata<String> metadata = OutgoingKafkaRecordMetadata.<String>builder()
			// 	.withTopic(bridgeId + "-" + channel)
			// 	.build();

			// producer.getToWebCmds().send(Message.of(event.toString()).addMetadata(metadata));

		} else if ("search_data".equals(channel)) {
			producer.getToSearchData().send(payload);

		} else {
			log.error("Producer unable to write to channel " + channel);
		}
	}
}
