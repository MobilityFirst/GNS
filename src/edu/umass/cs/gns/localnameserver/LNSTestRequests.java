package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.packet.NewActiveProposalPacket;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Used only during testing.
 * Sends special requests to name servers to test GNS functionality.
 *
 *
 * Created by abhigyan on 4/12/14.
 */
public class LNSTestRequests {

  /**
   * Sends a request to change the group of active replicas of a name.
   */
  public static void sendGroupChangeRequest(JSONObject jsonObject) throws JSONException {
    GNS.getLogger().fine("Sending group change packet: " + jsonObject);
    NewActiveProposalPacket packet = new NewActiveProposalPacket(jsonObject);
    LocalNameServer.sendToNS(jsonObject, packet.getProposingNode());
  }
}
