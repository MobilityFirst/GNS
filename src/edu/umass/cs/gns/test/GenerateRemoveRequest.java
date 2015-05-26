package edu.umass.cs.gns.test;

import edu.umass.cs.gns.newApp.clientCommandProcessor.demultSupport.CCPPacketDemultiplexerV1;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.newApp.packet.RemoveRecordPacket;
import org.json.JSONException;

import java.util.TimerTask;

/**
* Created by abhigyan on 5/21/14.
*/
class GenerateRemoveRequest<NodeIDType> extends TimerTask {

  private final int requestCount;
  private final String name;
  private final CCPPacketDemultiplexerV1 packetDemultiplexer;

  public GenerateRemoveRequest(String name, int count, CCPPacketDemultiplexerV1 packetDemultiplexer) {

    this.requestCount = count;
    this.name = name;
    this.packetDemultiplexer = packetDemultiplexer;

  }

  @Override
  public void run() {

    RemoveRecordPacket<NodeIDType> packet = new RemoveRecordPacket<NodeIDType>(null, requestCount, name, null);

    try {
      packetDemultiplexer.handleJSONObject(packet.toJSONObject());
    } catch (JSONException e) {
      GNS.getLogger().severe("Problem handling remove request:" + e);
    }
  }
}
