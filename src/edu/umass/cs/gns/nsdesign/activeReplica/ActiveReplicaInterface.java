package edu.umass.cs.gns.nsdesign.activeReplica;

import org.json.JSONObject;

/**
 * This interface exists so that packet demultiplexer can forward packets to active replica.
 * Created by abhigyan on 2/27/14.
 */
public interface ActiveReplicaInterface {

  public void handleIncomingPacket(JSONObject json);

  void resetDB();

}
