package edu.umass.cs.gns.test;

import edu.umass.cs.gns.clientsupport.UpdateOperation;
import edu.umass.cs.gns.localnameserver.LNSPacketDemultiplexer;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.packet.UpdatePacket;
import edu.umass.cs.gns.util.NameRecordKey;
import edu.umass.cs.gns.util.ResultValue;
import edu.umass.cs.gns.util.Util;
import org.json.JSONException;

import java.util.TimerTask;

/**
* Created by abhigyan on 5/21/14.
*/
class GenerateUpdateRequest extends TimerTask {

  private int updateCount;
  private String name;
  private LNSPacketDemultiplexer packetDemultiplexer;
  private int objectSizeBytes;

  public GenerateUpdateRequest(String name, int updateCount, int objectSizeBytes, LNSPacketDemultiplexer packetDemultiplexer) {

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
    UpdatePacket updateAddressPacket = new UpdatePacket(-1, updateCount, updateCount, name, NameRecordKey.EdgeRecord,
            newValue, null, -1, null, UpdateOperation.SINGLE_FIELD_REPLACE_ALL, -1, -1, GNS.DEFAULT_TTL_SECONDS, null, null, null);
    try {
      packetDemultiplexer.handleJSONObject(updateAddressPacket.toJSONObject());
    } catch (JSONException e) {
      e.printStackTrace();
    }
  }
}
