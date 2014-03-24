package edu.umass.cs.gns.nsdesign.replicaController;

import org.json.JSONObject;

/**
 * This interface exists so that packet demultiplexer can forward packets to replica controller.
 * Created by abhigyan on 2/27/14.
 */
public interface ReplicaControllerInterface {

  public void handleIncomingPacket(JSONObject json);

  void resetDB();
}
