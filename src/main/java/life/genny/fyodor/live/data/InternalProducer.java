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

  @Inject @Channel("search_eventsout") Emitter<String> searchEvents;
  public Emitter<String> getToSearchEvents() {
    return searchEvents;
  }

  @Inject @Channel("search_dataout") Emitter<String> searchData;
  public Emitter<String> getToSearchData() {
    return searchData;
  }

  @Inject @Channel("webcmdsout") Emitter<String> webCmds;
  public Emitter<String> getToWebCmds() {
    return webCmds;
  }

  @Inject @Channel("webdataout") Emitter<String> webData;
  public Emitter<String> getToWebData() {
    return webData;
  }

}
