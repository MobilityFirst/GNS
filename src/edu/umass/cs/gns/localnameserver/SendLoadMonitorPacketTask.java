package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartLocalNameServer;
import edu.umass.cs.gns.nsdesign.nodeconfig.GNSNodeConfig;
import edu.umass.cs.gns.nsdesign.nodeconfig.NodeId;
import edu.umass.cs.gns.nsdesign.packet.*;
import org.json.JSONException;

import java.util.TimerTask;

/**
 * @author abhigyan
 * @deprecated
 */
// FIXME: I assume this is unused since it's deprecated?
public class SendLoadMonitorPacketTask extends TimerTask {

  private NodeId<String> nameServerID;
  private NameServerLoadPacket nsLoad;

  public SendLoadMonitorPacketTask(NodeId<String> nsID) {
    nameServerID = nsID;
    nsLoad = new NameServerLoadPacket(GNSNodeConfig.INVALID_NAME_SERVER_ID, nsID, 0);
  }

  @Override
  public void run() {
    try {
      LocalNameServer.sendToNS(nsLoad.toJSONObject(), nameServerID);
    } catch (JSONException e) {
      e.printStackTrace();
    }
    if (StartLocalNameServer.debugMode) GNS.getLogger().fine("LoadMonitorPacketSent. NameServer:" + nsLoad.getReportingNodeID().get() +
            " Load:" + nsLoad.getLoadValue());
  }

}
