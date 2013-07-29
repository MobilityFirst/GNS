package edu.umass.cs.gns.packet;

import edu.umass.cs.gns.nameserver.NameRecordKey;
import org.json.JSONException;
import org.json.JSONObject;

public class RemoveReplicationRecordPacket extends BasicPacket {

  private final static String NAME = "name";
  //private final static String RECORDKEY = "recordkey";
  private final static String PRIMARY_NAMESERVER_ID = "sendingPrimary";

   /** Host/domain/device name **/
  private String name;
  /** The key of the value key pair. For GNRS this will be EdgeRecord, CoreRecord or GroupRecord. **/
  //private NameRecordKey recordKey;
  
  /** Id of primary nameserver sending this request **/
  private int primaryNameserverId;

  /*************************************************************
   * Constructs a new RemoveRecordPacket with the given name.
   * @param name Host/domain/device name
   * @param primaryNameserverId Id of primary nameserver sending
   * this request.
   ************************************************************/
  public RemoveReplicationRecordPacket(String name, int primaryNameserverId) {
    this.type = Packet.PacketType.REMOVE_REPLICATION_RECORD;
    this.name = name;
    //this.recordKey = recordKey;
    this.primaryNameserverId = primaryNameserverId;
  }

  /*************************************************************
   * Constructs a new RemoveRecordPacket from a JSONObject
   * @param json JSONObject that represents this packet
   * @throws JSONException
   ************************************************************/
  public RemoveReplicationRecordPacket(JSONObject json) throws JSONException {
    if (Packet.getPacketType(json) != Packet.PacketType.REMOVE_REPLICATION_RECORD) {
      Exception e = new Exception("RemoveReplicationRecordPacket: wrong packet type " + Packet.getPacketType(json));
      e.printStackTrace();
    }
    this.type = Packet.PacketType.REMOVE_REPLICATION_RECORD;
    this.name = json.getString(NAME);
    //this.recordKey = NameRecordKey.valueOf(json.getString(RECORDKEY));
    this.primaryNameserverId = json.getInt(PRIMARY_NAMESERVER_ID);
  }

  /*************************************************************
   * Converts RemoveRecordPacket object to a JSONObject
   * @return JSONObject that represents this packet
   * @throws JSONException
   ************************************************************/
  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    Packet.putPacketType(json, getType());
    //json.put(RECORDKEY, getRecordKey().getName());
    json.put(NAME, getName());
    json.put(PRIMARY_NAMESERVER_ID, getPrimaryNameserverId());
    return json;
  }

  /**
   * @return the name
   */
  public String getName() {
    return name;
  }

//  /**
//   * @return the recordKey
//   */
//  public NameRecordKey getRecordKey() {
//    return recordKey;
//  }

  /**
   * @return the primaryNameserverId
   */
  public int getPrimaryNameserverId() {
    return primaryNameserverId;
  }
}
