package edu.umass.cs.gns.nsdesign.gnsReconfigurable;

import edu.umass.cs.gns.exceptions.FailedDBOperationException;
import edu.umass.cs.gns.exceptions.RecordExistsException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.Config;
import edu.umass.cs.gns.nsdesign.packet.AddRecordPacket;
import edu.umass.cs.gns.nsdesign.packet.Packet;
import edu.umass.cs.gns.nsdesign.recordmap.NameRecord;
import edu.umass.cs.gns.util.ValuesMap;
import org.json.JSONException;

import java.io.IOException;

/**
 * Contains code executed by an active replica on adding a new name to GNS.
 * Created by abhigyan on 2/27/14.
 */
public class Add {

  public static void handleActiveAdd(AddRecordPacket addRecordPacket, GnsReconfigurable gnsApp)
          throws JSONException, IOException, FailedDBOperationException {

    if (Config.debugMode) GNS.getLogger().fine("Add record at active replica. name = " + addRecordPacket.getName() +
            " node id: " + gnsApp.getNodeID());
    ValuesMap valuesMap = new ValuesMap();
    valuesMap.putAsArray(addRecordPacket.getRecordKey().getName(), addRecordPacket.getValue());

    NameRecord nameRecord = new NameRecord(gnsApp.getDB(), addRecordPacket.getName(), Config.FIRST_VERSION,
            valuesMap, addRecordPacket.getTTL());
    try {
      NameRecord.addNameRecord(gnsApp.getDB(), nameRecord);

    } catch (RecordExistsException e) {
      // todo this case should happen rarely if we actually delete record at the end of remove operation
      try {
        NameRecord.removeNameRecord(gnsApp.getDB(), addRecordPacket.getName());
        NameRecord.addNameRecord(gnsApp.getDB(), nameRecord);
      } catch (RecordExistsException e1) {
        GNS.getLogger().severe("Name record exists when we just deleted it!!! - " + e.getMessage());
        e1.printStackTrace();
      }
      if (Config.debugMode) {
        GNS.getLogger().fine("Name record already exists, i.e., record deleted and reinserted. Here is the inserted record: " + nameRecord);
      }
    }
    sendConfirmMsg(addRecordPacket, gnsApp);
  }

  /**
   * Adds a new <code>NameRecord</code> to database, and replies to <code>ReplicaController</code> on the same node
   * that the record is added.
   *
   * @param addRecordPacket <code>AddRecordPacket</code> sent by <code>ReplicaController</code>
   * @param gnsApp <code>GnsReconfigurable</code> calling this method
   * @throws JSONException
   */
  private static void sendConfirmMsg(AddRecordPacket addRecordPacket, GnsReconfigurable gnsApp)
          throws JSONException, IOException {
    addRecordPacket.setType(Packet.PacketType.ACTIVE_ADD_CONFIRM);
    gnsApp.getNioServer().sendToID(gnsApp.getNodeID(), addRecordPacket.toJSONObject());
  }

}
