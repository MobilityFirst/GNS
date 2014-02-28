package edu.umass.cs.gns.nsdesign.replicaController;

import org.json.JSONObject;

/**
 * Created by abhigyan on 2/27/14.
 */
public interface ReplicaControllerInterface {

  public void handleIncomingPacket(JSONObject json);

  public void executeRequestLocal(JSONObject json);
}
