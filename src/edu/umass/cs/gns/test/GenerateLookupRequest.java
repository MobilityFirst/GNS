package edu.umass.cs.gns.test;

import edu.umass.cs.gns.database.ColumnFieldType;
import edu.umass.cs.gns.clientCommandProcessor.CCPPacketDemultiplexerV1;
import edu.umass.cs.gns.nsdesign.packet.DNSPacket;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.TimerTask;

/**
 * Created by abhigyan on 5/21/14.
 */
public class GenerateLookupRequest<NodeIDType> extends TimerTask {

  private int lookupCount;
  private String name;
  private CCPPacketDemultiplexerV1 packetDemultiplexer;

  public GenerateLookupRequest(String name, int lookupCount, CCPPacketDemultiplexerV1 packetDemultiplexer) {

    this.lookupCount = lookupCount;
    this.name = name;
    this.packetDemultiplexer = packetDemultiplexer;
  }

  @Override
  public void run() {
    DNSPacket<NodeIDType> queryRecord = new DNSPacket<NodeIDType>(null, lookupCount, name, 
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
