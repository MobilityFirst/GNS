package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.packet.GroupChangeCompletePacket;
import edu.umass.cs.gns.nsdesign.packet.NewActiveProposalPacket;
import edu.umass.cs.gns.util.GroupChangeIdentifier;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Used only during testing.
 * Sends special requests to name servers to test GNS functionality.
 *
 * Currently, sends requests and receives confirmation from NS regarding group changes.
 *
 * Created by abhigyan on 4/12/14.
 */
public class LNSTestRequests {

  /**
   * Stores info about group change requests this client has sent.
   * Key is the identifier for the group change, and value is the time when local name server sent the request.
   * Note there is no retransmission for this request. Also, some requests may not received responses,
   * and state for those may lie around in this object for the remainder of the test.
   */
  private static ConcurrentHashMap<GroupChangeIdentifier, Long> trackGroupChange =
          new ConcurrentHashMap<GroupChangeIdentifier, Long>();
  /**
   * Sends a request to change the group of active replicas of a name.
   */
  public static void sendGroupChangeRequest(JSONObject jsonObject) throws JSONException {
    GNS.getLogger().fine("Sending group change packet: " + jsonObject);
    NewActiveProposalPacket packet = new NewActiveProposalPacket(jsonObject);
    packet.setLnsAddress(LocalNameServer.getNodeAddress());
    LocalNameServer.sendToNS(packet.toJSONObject(), packet.getProposingNode());
    trackGroupChange.put(new GroupChangeIdentifier(packet.getName(), packet.getVersion()), System.currentTimeMillis());
  }

  /**
   * Handles confirmation from name server that group change in complete.
   */
  public static void handleGroupChangeComplete(JSONObject json) throws JSONException {
    GNS.getLogger().fine("Received group change complete packet: " + json);
    GroupChangeCompletePacket gcp = new GroupChangeCompletePacket(json);
    GroupChangeIdentifier gci = new GroupChangeIdentifier(gcp.getName(), gcp.getVersion());
    Long sendTime = trackGroupChange.remove(gci);
    if (sendTime != null) {
      String message = "Success-GroupChange" + "\t" + gcp.getName() + "\t" + gcp.getVersion()  + "\t" + (System.currentTimeMillis() - sendTime)  + "\t";
      GNS.getStatLogger().info(message);
    } else {
      // this cannot happen as we are not retransmitting the request (so we can get at most one response)
      GNS.getLogger().severe("ERROR: Group change confirmation at LNS but no state for request." + json);
    }

  }
}
