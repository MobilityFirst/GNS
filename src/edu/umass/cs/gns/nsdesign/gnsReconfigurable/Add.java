package edu.umass.cs.gns.nsdesign.gnsReconfigurable;


import edu.umass.cs.gns.exceptions.RecordExistsException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.util.ValuesMap;
import edu.umass.cs.gns.nsdesign.recordmap.NameRecord;
import edu.umass.cs.gns.nsdesign.GNSMessagingTask;
import edu.umass.cs.gns.nsdesign.packet.AddRecordPacket;
import edu.umass.cs.gns.nsdesign.packet.Packet;
import edu.umass.cs.gns.util.ConsistentHashing;
import org.json.JSONException;

/**
 * Contains code executed by an active replica on adding a new name to GNS.
 * Created by abhigyan on 2/27/14.
 */
public class Add {

  public static GNSMessagingTask handleActiveAdd(AddRecordPacket addRecordPacket, GnsReconfigurable activeReplica)
          throws JSONException {

    GNSMessagingTask msgTask = null;
    GNS.getLogger().fine("Add record at Active replica. name = " + addRecordPacket.getName() + " node id: " +
            activeReplica.getNodeID());
    ValuesMap valuesMap = new ValuesMap();
    valuesMap.put(addRecordPacket.getRecordKey().getName(), addRecordPacket.getValue());

    NameRecord nameRecord = new NameRecord(activeReplica.getDB(), addRecordPacket.getName(),
            ConsistentHashing.getReplicaControllerSet(addRecordPacket.getName()), 1, valuesMap,
            addRecordPacket.getTTL());

    try {
      NameRecord.addNameRecord(activeReplica.getDB(), nameRecord);

      try {
        String val = NameRecord.getNameRecord(activeReplica.getDB(), addRecordPacket.getName()).toString();
        GNS.getLogger().fine("Name record read: " + val);
      } catch (RecordNotFoundException e) {
        e.printStackTrace();
      }

    } catch (RecordExistsException e) {
      NameRecord.removeNameRecord(activeReplica.getDB(), addRecordPacket.getName());
      try {
        NameRecord.addNameRecord(activeReplica.getDB(), nameRecord);
      } catch (RecordExistsException e1) {
        e1.printStackTrace();
      }
      GNS.getLogger().fine("Name record already exists, i.e., record deleted and reinserted.");
    }

    // this will create state needed for coordination
    if (activeReplica.getActiveCoordinator() != null) {
      activeReplica.getActiveCoordinator().coordinateRequest(addRecordPacket.toJSONObject());
    } else {
      msgTask = executeSendConfirmation(addRecordPacket, activeReplica);
    }
    return msgTask;
  }

  /**
   * Adds a new <code>NameRecord</code> to database, and replies to <code>ReplicaController</code> on the same node
   * that the record is added.
   * @param addRecordPacket <code>AddRecordPacket</code> sent by <code>ReplicaController</code>
   * @param activeReplica <code>GnsReconfigurable</code> calling this method
   * @return GNSMessagingTask to send to replica controller on same node.
   * @throws JSONException
   */
  public static GNSMessagingTask executeSendConfirmation(AddRecordPacket addRecordPacket, GnsReconfigurable activeReplica)
          throws JSONException {

    addRecordPacket.setType(Packet.PacketType.ACTIVE_ADD_CONFIRM);
    return new GNSMessagingTask(activeReplica.getNodeID(), addRecordPacket.toJSONObject());
  }


}
