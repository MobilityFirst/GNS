package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartLocalNameServer;
import edu.umass.cs.gns.nsdesign.nodeconfig.GNSNodeConfig;
import edu.umass.cs.gns.nsdesign.packet.*;
import org.json.JSONException;

import java.util.TimerTask;

/**
 * @author abhigyan
 * @param <NodeIDType>
 * @deprecated
 */
// FIXME: I assume this is unused since it's deprecated?
public class SendLoadMonitorPacketTask<NodeIDType> extends TimerTask {

  private final NodeIDType nameServerID;
  private final NameServerLoadPacket nsLoad;

  public SendLoadMonitorPacketTask(NodeIDType nsID) {
    nameServerID = nsID;
    nsLoad = new NameServerLoadPacket(null, nsID, 0);
  }

  @Override
  public void run() {
    try {
      LocalNameServer.sendToNS(nsLoad.toJSONObject(), (String) nameServerID);
    } catch (JSONException e) {
      e.printStackTrace();
    }
    if (StartLocalNameServer.debuggingEnabled) GNS.getLogger().fine("LoadMonitorPacketSent. NameServer:" + nsLoad.getReportingNodeID().toString() +
            " Load:" + nsLoad.getLoadValue());
  }

}
