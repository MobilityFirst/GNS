package edu.umass.cs.gns.test;

import edu.umass.cs.gns.localnameserver.LNSPacketDemultiplexer;
import edu.umass.cs.gns.nsdesign.packet.AddRecordPacket;
import edu.umass.cs.gns.util.ResultValue;
import edu.umass.cs.gns.util.Util;
import org.json.JSONException;

import java.util.Set;
import java.util.TimerTask;

/**
* Created by abhigyan on 5/21/14.
*/
class GenerateAddRequest extends TimerTask {

  private int requestCount;
  private String name;
  private LNSPacketDemultiplexer packetDemux;
  private int objectSizeKB;
  private int ttl;

  private Set<Integer> activeNameServers;

  public GenerateAddRequest(String name, int count, int objectSizeBytes, int ttl, LNSPacketDemultiplexer packetDemux,
                            Set<Integer> activeNameServers) {
    this.requestCount = count;
    this.name = name;
    this.packetDemux = packetDemux;
    this.objectSizeKB = objectSizeBytes;
    this.ttl = ttl;
    this.activeNameServers = activeNameServers;
  }

  public GenerateAddRequest(String name, int count, int objectSizeBytes, int ttl, LNSPacketDemultiplexer packetDemux) {
    this(name,count, objectSizeBytes, ttl, packetDemux, null);
  }

  @Override
  public void run() {

    ResultValue newValue = new ResultValue();
    newValue.add(Util.randomString(objectSizeKB));
    AddRecordPacket packet = new AddRecordPacket(-1, requestCount, name, "EdgeRecord", newValue, -1, ttl);

    if (activeNameServers != null) packet.setActiveNameSevers(activeNameServers);

    try {
      packetDemux.handleJSONObject(packet.toJSONObject());
    } catch (JSONException e) {
      e.printStackTrace();
    }
  }

}
