package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartLocalNameServer;
import edu.umass.cs.gns.nsdesign.packet.*;
import org.json.JSONException;

import java.util.TimerTask;

public class SendLoadMonitorPacketTask extends TimerTask {

  private int nameServerID;
  private NameServerLoadPacket nsLoad;

  public SendLoadMonitorPacketTask(int nsID) {
    nameServerID = nsID;
    nsLoad = new NameServerLoadPacket(LocalNameServer.getNodeID(), nsID, 0);
  }

  @Override
  public void run() {
    try {
      LocalNameServer.sendToNS(nsLoad.toJSONObject(), nameServerID);
    } catch (JSONException e) {
      e.printStackTrace();
    }
    if (StartLocalNameServer.debugMode) GNS.getLogger().fine("LoadMonitorPacketSent. NameServer:" + nsLoad.getNsID() +
            " Load:" + nsLoad.getLoadValue());
  }

}
