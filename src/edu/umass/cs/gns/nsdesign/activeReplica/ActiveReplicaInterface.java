package edu.umass.cs.gns.nsdesign.activeReplica;

import org.json.JSONObject;

/**
 * Created by abhigyan on 2/27/14.
 */
public interface ActiveReplicaInterface {

  public void handleIncomingPacket(JSONObject json);

  public void executeRequestLocal(JSONObject json);

}
