package edu.umass.cs.gns.test;

import edu.umass.cs.gns.localnameserver.LNSPacketDemultiplexer;
import edu.umass.cs.gns.nsdesign.nodeconfig.GNSNodeConfig;
import edu.umass.cs.gns.nsdesign.packet.RemoveRecordPacket;
import org.json.JSONException;

import java.util.TimerTask;

/**
* Created by abhigyan on 5/21/14.
*/
class GenerateRemoveRequest extends TimerTask {

  private int requestCount;
  private String name;
  private LNSPacketDemultiplexer packetDemultiplexer;

  public GenerateRemoveRequest(String name, int count, LNSPacketDemultiplexer packetDemultiplexer) {

    this.requestCount = count;
    this.name = name;
    this.packetDemultiplexer = packetDemultiplexer;

  }

  @Override
  public void run() {

    RemoveRecordPacket packet = new RemoveRecordPacket(GNSNodeConfig.INVALID_NAME_SERVER_ID, requestCount, name, null);

    try {
      packetDemultiplexer.handleJSONObject(packet.toJSONObject());
    } catch (JSONException e) {
      e.printStackTrace();
    }
  }
}
