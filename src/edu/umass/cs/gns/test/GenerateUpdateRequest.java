package edu.umass.cs.gns.test;

import edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.UpdateOperation;
import edu.umass.cs.gns.newApp.clientCommandProcessor.demultSupport.CCPPacketDemultiplexerV1;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.packet.UpdatePacket;
import edu.umass.cs.gns.util.ResultValue;
import edu.umass.cs.gns.util.Util;
import org.json.JSONException;

import java.util.TimerTask;

/**
* Created by abhigyan on 5/21/14.
*/
class GenerateUpdateRequest<NodeIDType> extends TimerTask {

  private final int updateCount;
  private final String name;
  private final CCPPacketDemultiplexerV1 packetDemultiplexer;
  private final int objectSizeBytes;

  public GenerateUpdateRequest(String name, int updateCount, int objectSizeBytes, CCPPacketDemultiplexerV1 packetDemultiplexer) {

    this.updateCount = updateCount;
    this.name = name;
    this.packetDemultiplexer = packetDemultiplexer;
    this.objectSizeBytes = objectSizeBytes;
  }

  @Override
  public void run() {

    ResultValue newValue = new ResultValue();
    newValue.add(Util.randomString(objectSizeBytes));
    //ignore signature info
    UpdatePacket<NodeIDType> updateAddressPacket = new UpdatePacket<NodeIDType>(null, updateCount, updateCount, name, "EdgeRecord",
            newValue, null, -1, null, UpdateOperation.SINGLE_FIELD_REPLACE_ALL, null, null, GNS.DEFAULT_TTL_SECONDS, null, null, null);
    try {
      packetDemultiplexer.handleJSONObject(updateAddressPacket.toJSONObject());
    } catch (JSONException e) {
       GNS.getLogger().severe("Problem handling update request:" + e);
    }
  }
}
