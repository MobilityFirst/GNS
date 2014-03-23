package edu.umass.cs.gns.nsdesign.activeReplica;


import edu.umass.cs.gns.exceptions.RecordExistsException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nameserver.recordmap.NameRecord;
import edu.umass.cs.gns.nameserver.ValuesMap;
import edu.umass.cs.gns.nsdesign.GNSMessagingTask;
import edu.umass.cs.gns.packet.AddRecordPacket;
import edu.umass.cs.gns.packet.Packet;
import edu.umass.cs.gns.util.ConsistentHashing;
import org.json.JSONException;

/**
 * Contains code executed by an active replica on adding a new name to GNS.
 * Created by abhigyan on 2/27/14.
 */
public class Add {

  /**
   * Adds a new <code>NameRecord</code> to database, and replies to <code>ReplicaController</code> on the same node
   * that the record is added.
   * @param addRecordPacket <code>AddRecordPacket</code> sent by <code>ReplicaController</code>
   * @param activeReplica <code>ActiveReplica</code> calling this method
   * @return GNSMessagingTask to send to replica controller on same node.
   * @throws JSONException
   */
  public static GNSMessagingTask executeAddRecord(AddRecordPacket addRecordPacket, ActiveReplica activeReplica)
          throws JSONException {

    GNS.getLogger().info("Add record at Active replica. name = " + addRecordPacket.getName());
    ValuesMap valuesMap = new ValuesMap();
    valuesMap.put(addRecordPacket.getRecordKey().getName(), addRecordPacket.getValue());

    NameRecord nameRecord = new NameRecord(activeReplica.getDB(), addRecordPacket.getName(),
            ConsistentHashing.getReplicaControllerSet(addRecordPacket.getName()),
            addRecordPacket.getName() + "-1", valuesMap, addRecordPacket.getTTL());
    try {
      NameRecord.addNameRecord(activeReplica.getDB(), nameRecord);

    } catch (RecordExistsException e) {
      GNS.getLogger().severe("ERROR: Exception: name record already exists! This should never happen ");
      e.printStackTrace();
    }
    addRecordPacket.setType(Packet.PacketType.ACTIVE_ADD_CONFIRM);
    return new GNSMessagingTask(activeReplica.getNodeID(), addRecordPacket.toJSONObject());
  }

}
