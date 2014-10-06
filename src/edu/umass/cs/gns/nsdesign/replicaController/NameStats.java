package edu.umass.cs.gns.nsdesign.replicaController;

import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.Config;
import edu.umass.cs.gns.nsdesign.packet.NameServerSelectionPacket;
import edu.umass.cs.gns.nsdesign.recordmap.ReplicaControllerRecord;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * On receiving statistics for a name such as read rate, write rate, name servers' votes,
 * this method updates the database at replica controllers. These statistics are used to decide active replicas
 * for that name.
 * <p>
 * Created by abhigyan on 2/27/14.
 */
public class NameStats {

  /**
   * Updates statistics in replica controller record for a name based on stats sent by local name server, including
   * read rate, write rate, and votes for closest name server.
   */
  public static void handleLNSVotesPacket(JSONObject incomingJSON, ReplicaController replicaController) throws JSONException {
    if (Config.debuggingEnabled) GNS.getLogger().fine("NS: received  NAMESERVER_SELECTION " + incomingJSON.toString());
    NameServerSelectionPacket selectionPacket = new NameServerSelectionPacket(incomingJSON);
    try {
      ReplicaControllerRecord rcRecord = new ReplicaControllerRecord(replicaController.getDB(), selectionPacket.getName());
      try {
        rcRecord.addReplicaSelectionVote(selectionPacket.getNameServerID(), selectionPacket.getVote(),selectionPacket.getUpdate());
      } catch (FieldNotFoundException e) {
        GNS.getLogger().severe("Field not found exception. " + e.getMessage());
        e.printStackTrace();
      }

    } catch (Exception e) {
      GNS.getLogger().severe("Exception here " + e.getMessage());
      e.printStackTrace();
    }
  }
}
