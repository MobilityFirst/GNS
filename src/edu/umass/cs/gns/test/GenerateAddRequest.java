package edu.umass.cs.gns.test;

import edu.umass.cs.gns.localnameserver.LNSPacketDemultiplexer;
import edu.umass.cs.gns.nsdesign.packet.AddRecordPacket;
import edu.umass.cs.gns.util.NameRecordKey;
import edu.umass.cs.gns.util.ResultValue;
import edu.umass.cs.gns.util.Util;
import org.json.JSONException;

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

  public GenerateAddRequest(String name, int count, int objectSizeBytes, int ttl, LNSPacketDemultiplexer packetDemux) {

    this.requestCount = count;
    this.name = name;
    this.packetDemux = packetDemux;
    this.objectSizeKB = objectSizeBytes;
    this.ttl = ttl;
  }

  @Override
  public void run() {

    ResultValue newValue = new ResultValue();
    newValue.add(Util.randomString(objectSizeKB));
    AddRecordPacket packet = new AddRecordPacket(-1, requestCount, name, NameRecordKey.EdgeRecord, newValue,
            -1, ttl);

    try {
      packetDemux.handleJSONObject(packet.toJSONObject());
    } catch (JSONException e) {
      e.printStackTrace();
    }
  }

}
