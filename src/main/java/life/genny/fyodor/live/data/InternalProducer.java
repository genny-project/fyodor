package life.genny.fyodor.live.data;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;

/**
 * InternalProducer --- Kafka smalltye producer objects to send to internal consumers backends
 * such as wildfly-rulesservice. 
 */

@ApplicationScoped
public class InternalProducer {

  @Inject @Channel("search_eventsout") Emitter<String> events;
  public Emitter<String> getToEvents() {
    return events;
  }

  @Inject @Channel("search_dataout") Emitter<String> data;
  public Emitter<String> getToData() {
    return data;
  }

}
