package edu.umass.cs.gns.localnameserver;

import org.json.JSONObject;

/**
 * Intercessor is an object to which a local name server forward the final response for all requests. These responses
 * are subsequently sent to clients. The interface for an intercessor is given below.
 *
 * The intercessor for the GNS client library is in {@link edu.umass.cs.gns.clientsupport.Intercessor}
 *
 * For test purposes, we have developed an alternate client {@link edu.umass.cs.gns.test.nioclient.DBClient}
 * The intercessor which forwards responses to DBClient is in {@link edu.umass.cs.gns.test.nioclient.DBClientIntercessor}
 *
 * Created by abhigyan on 6/19/14.
 */
public interface IntercessorInterface {

  /**
   * Handle packets coming in to the Local Name Server.
   * 
   * @param jsonObject
   */
  public void handleIncomingPacket(JSONObject jsonObject);
}
