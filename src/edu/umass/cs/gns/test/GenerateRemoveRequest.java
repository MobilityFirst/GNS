package edu.umass.cs.gns.test;

import edu.umass.cs.gns.localnameserver.LNSPacketDemultiplexer;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.packet.RemoveRecordPacket;
import org.json.JSONException;

import java.util.TimerTask;

/**
* Created by abhigyan on 5/21/14.
*/
class GenerateRemoveRequest extends TimerTask {

  private final int requestCount;
  private final String name;
  private final LNSPacketDemultiplexer packetDemultiplexer;

  public GenerateRemoveRequest(String name, int count, LNSPacketDemultiplexer packetDemultiplexer) {

    this.requestCount = count;
    this.name = name;
    this.packetDemultiplexer = packetDemultiplexer;

  }

  @Override
  public void run() {

    RemoveRecordPacket packet = new RemoveRecordPacket(null, requestCount, name, null);

    try {
      packetDemultiplexer.handleJSONObject(packet.toJSONObject());
    } catch (JSONException e) {
      GNS.getLogger().severe("Problem handling remove request:" + e);
    }
  }
}
