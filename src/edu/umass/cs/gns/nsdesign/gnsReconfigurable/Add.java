package edu.umass.cs.gns.nsdesign.gnsReconfigurable;

import edu.umass.cs.gns.exceptions.FailedUpdateException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.Config;
import edu.umass.cs.gns.nsdesign.GNSMessagingTask;
import edu.umass.cs.gns.nsdesign.packet.AddRecordPacket;
import edu.umass.cs.gns.nsdesign.packet.Packet;
import edu.umass.cs.gns.nsdesign.recordmap.NameRecord;
import edu.umass.cs.gns.util.ValuesMap;
import org.json.JSONException;

/**
 * Contains code executed by an active replica on adding a new name to GNS.
 * Created by abhigyan on 2/27/14.
 */
public class Add {

  public static GNSMessagingTask handleActiveAdd(AddRecordPacket addRecordPacket, GnsReconfigurable activeReplica)
          throws JSONException {

    GNS.getLogger().fine("Add record at Active replica. name = " + addRecordPacket.getName() + " node id: "
            + activeReplica.getNodeID());
    ValuesMap valuesMap = new ValuesMap();
    valuesMap.put(addRecordPacket.getRecordKey().getName(), addRecordPacket.getValue());

    NameRecord nameRecord = new NameRecord(activeReplica.getDB(), addRecordPacket.getName(), Config.FIRST_VERSION,
            valuesMap, addRecordPacket.getTTL());
    try {
      NameRecord.addNameRecord(activeReplica.getDB(), nameRecord);
      try {
        String val = NameRecord.getNameRecord(activeReplica.getDB(), addRecordPacket.getName()).toString();
        GNS.getLogger().fine("Name record read: " + val);
      } catch (RecordNotFoundException e) {
        e.printStackTrace();
      }
    } catch (FailedUpdateException e) {
      // todo this case should happen rarely if we actually delete record at the end of remove operation
      try {
        NameRecord.removeNameRecord(activeReplica.getDB(), addRecordPacket.getName());
        NameRecord.removeNameRecord(activeReplica.getDB(), addRecordPacket.getName());
        NameRecord.addNameRecord(activeReplica.getDB(), nameRecord);
      } catch (FailedUpdateException e1) {
        GNS.getLogger().severe("Failed update exception:" + e.getMessage());
        e1.printStackTrace();
      }
      GNS.getLogger().fine("Name record already exists, i.e., record deleted and reinserted.");
    }

    return getConfirmMsg(addRecordPacket, activeReplica);
  }

  /**
   * Adds a new <code>NameRecord</code> to database, and replies to <code>ReplicaController</code> on the same node
   * that the record is added.
   *
   * @param addRecordPacket <code>AddRecordPacket</code> sent by <code>ReplicaController</code>
   * @param activeReplica <code>GnsReconfigurable</code> calling this method
   * @return GNSMessagingTask to send to replica controller on same node.
   * @throws JSONException
   */
  private static GNSMessagingTask getConfirmMsg(AddRecordPacket addRecordPacket, GnsReconfigurable activeReplica)
          throws JSONException {

    addRecordPacket.setType(Packet.PacketType.ACTIVE_ADD_CONFIRM);
    return new GNSMessagingTask(activeReplica.getNodeID(), addRecordPacket.toJSONObject());
  }

}
