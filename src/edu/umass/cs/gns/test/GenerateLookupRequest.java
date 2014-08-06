package edu.umass.cs.gns.test;

import edu.umass.cs.gns.database.ColumnFieldType;
import edu.umass.cs.gns.localnameserver.LNSPacketDemultiplexer;
import edu.umass.cs.gns.nsdesign.packet.DNSPacket;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.TimerTask;

/**
 * Created by abhigyan on 5/21/14.
 */
public class GenerateLookupRequest extends TimerTask {

  private int lookupCount;
  private String name;
  private LNSPacketDemultiplexer packetDemultiplexer;

  public GenerateLookupRequest(String name, int lookupCount, LNSPacketDemultiplexer packetDemultiplexer) {

    this.lookupCount = lookupCount;
    this.name = name;
    this.packetDemultiplexer = packetDemultiplexer;
  }

  @Override
  public void run() {
    DNSPacket queryRecord = new DNSPacket(-1, lookupCount, name, 
            "EdgeRecord", null,
            ColumnFieldType.LIST_STRING, null, null, null);
    queryRecord.getHeader().setId(lookupCount);

    JSONObject json;
    try {
      json = queryRecord.toJSONObjectQuestion();
      packetDemultiplexer.handleJSONObject(json);
    } catch (JSONException e) {
      e.printStackTrace();

    }

  }
}
