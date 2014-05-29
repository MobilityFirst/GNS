package edu.umass.cs.gns.nio;

import org.json.JSONObject;

public abstract class BasicPacketDemultiplexer {

  /**
   * The return value should return true if the handler
   * handled the message and doesn't want any other BasicPacketDemultiplexer
   * to handle the message.
   *
   * @param jsonObject
   * @return
   */
  public abstract boolean handleJSONObject(JSONObject jsonObject);

  public void incrPktsRcvd() {
    NIOInstrumenter.incrPktsRcvd();
  } // Used for testing and debugging

}
