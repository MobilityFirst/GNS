package edu.umass.cs.gns.gnsApp.clientCommandProcessor.demultSupport;

import org.json.JSONObject;

/**
 * Intercessor is an object to which a local name server forward the final response for all requests. These responses
 * are subsequently sent to clients. The interface for an intercessor is given below.
 *
 * The intercessor for the GNS client library is in {@link edu.umass.cs.gns.gnsApp.clientCommandProcessor.commandSupport.Intercessor}
 *
 */
public interface IntercessorInterface {

  /**
   * Handle packets coming in to the Local Name Server.
   * 
   * @param jsonObject
   */
  public void handleIncomingPacket(JSONObject jsonObject);
}
