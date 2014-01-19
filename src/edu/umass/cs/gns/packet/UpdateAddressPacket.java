package edu.umass.cs.gns.packet;

import edu.umass.cs.gns.client.UpdateOperation;
import edu.umass.cs.gns.nameserver.ResultValue;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nameserver.NameRecordKey;
import edu.umass.cs.gns.util.JSONUtils;
import net.sourceforge.sizeof.SizeOf;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * ***********************************************************
 * 
 * Implements the packet transmitted from a local name server to update the name to value mapping.
 *
 * @author Hardeeep, Westy
 */
public class UpdateAddressPacket extends BasicPacket {

//  private final static String SEQUENCENUMBER = "sequencenumber";
  private final static String REQUESTID = "reqID";
  private final static String LocalNSREQUESTID = "LNSreqID";
  private final static String NameServerREQUESTID = "NSreqID";
  private final static String NAME = "name";
  private final static String RECORDKEY = "recordkey";
  private final static String NEWVALUE = "newvalue";
  private final static String OLDVALUE = "oldvalue";
  private final static String NAMESERVER_ID = "nsID";
  private final static String LOCAL_NAMESERVER_ID = "lnsID";
  private final static String TTL = "ttl";
  private final static String OPERATION = "operation";
  //
  // NOTE: CHANGED THE IDS A BIT - Westy
  // We have three. First one is used by the entity making the initial request (often the intercessor).
  // Second is used by the LNS to keep track if it's update records.
  // Third is used by the NS
  //
  /**
   * Unique identifier used by the entity making the initial request to confirm
   */
  private int requestID;
  /**
   * The ID the LNS uses to for bookkeeping
   */
  private int LNSRequestID;
  /**
   * The ID the NS uses to for bookkeeping
   */
  private int NSRequestID;
  /**
   * The key of the value key pair. For GNRS this will be EdgeRecord, CoreRecord or GroupRecord.
   */
  //
  private NameRecordKey recordKey;
  /**
   * Name (service/host/domain or device name) *
   */
  private String name;
  /**
   * Value for updating *
   */
  private ResultValue updateValue;
  /**
   * The old value to replace in a substitute operation *
   */
  private ResultValue oldValue;
  /**
   * The operation to perform *
   */
  private UpdateOperation operation;
  /**
   * Local name server transmitting this packet *
   */
  private int localNameServerId;
  /**
   * Name server transmitting this packet *
   */
  private int nameServerId;
  /**
   * Time to live
   */
  private int ttl;

  /**
   * ***********************************************************
   * Constructs a new UpdateAddressPacket with the given parameters.
   *
   * @param type Type of this packet
   * @param name Name (service/host/domain or device name)
   * @param newValue Updated address
   * @param oldValue Old address to be replaced (if applicable, can be null)
   */
  public UpdateAddressPacket(Packet.PacketType type, int requestID, String name, NameRecordKey recordKey,
          ResultValue newValue, ResultValue oldValue, UpdateOperation operation, int localNameServerId, int ttl) {
    this(type, requestID, -1, -1, name, recordKey, newValue, oldValue, operation, localNameServerId, -1, ttl);
  }

  public UpdateAddressPacket(Packet.PacketType type,
          int requestID, int LNSRequestID, int NSRequestID,
          String name, NameRecordKey recordKey,
          ResultValue newValue,
          ResultValue oldValue,
          UpdateOperation operation,
          int localNameServerId, int nameServerId, int ttl) {
    this.type = type;
    this.requestID = requestID;
    this.LNSRequestID = LNSRequestID;
    this.NSRequestID = NSRequestID;
    this.name = name;
    this.recordKey = recordKey;
    this.operation = operation;
    this.updateValue = newValue;
    this.oldValue = oldValue;
    this.localNameServerId = localNameServerId;
    this.nameServerId = nameServerId;
    this.ttl = ttl;
  }

  /**
   * ***********************************************************
   * Constructs a new UpdateAddressPacket from a JSONObject.
   *
   * @param json JSONObject that represents UpdatedAddressPacket.
   * @throws JSONException **********************************************************
   */
  public UpdateAddressPacket(JSONObject json) throws JSONException {
    this.type = Packet.getPacketType(json);
    this.requestID = json.getInt(REQUESTID);
    this.LNSRequestID = json.getInt(LocalNSREQUESTID);
    this.NSRequestID = json.getInt(NameServerREQUESTID);
    this.name = json.getString(NAME);
    this.recordKey = NameRecordKey.valueOf(json.getString(RECORDKEY));
    this.operation = UpdateOperation.valueOf(json.getString(OPERATION));
    this.updateValue = JSONUtils.JSONArrayToResultValue(json.getJSONArray(NEWVALUE));
    this.oldValue = json.has(OLDVALUE) ? JSONUtils.JSONArrayToResultValue(json.getJSONArray(OLDVALUE)) : null;
    this.localNameServerId = json.getInt(LOCAL_NAMESERVER_ID);
    this.nameServerId = json.getInt(NAMESERVER_ID);
    this.ttl = json.getInt(TTL);
  }

  /**
   * ***********************************************************
   * Converts a UpdatedAddressPacket to a JSONObject
   *
   * @return JSONObject that represents UpdatedAddressPacket
   * @throws JSONException **********************************************************
   */
  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    Packet.putPacketType(json, getType());
    json.put(REQUESTID, getRequestID());
    json.put(LocalNSREQUESTID, getLNSRequestID());
    json.put(NameServerREQUESTID, getNSRequestID());
    json.put(NAME, getName());
    json.put(RECORDKEY, getRecordKey().getName());
    json.put(OPERATION, getOperation().name());
    json.put(NEWVALUE, new JSONArray(getUpdateValue()));
    if (getOldValue() != null) {
      json.put(OLDVALUE, new JSONArray(getOldValue()));
    }
    json.put(LOCAL_NAMESERVER_ID, getLocalNameServerId());
    json.put(NAMESERVER_ID, getNameServerId());
    json.put(TTL, getTTL());
    return json;
  }

  public int getRequestID() {
    return requestID;
  }

  /**
   * @return the id which is used by the LNS for bookkeeping
   */
  public int getLNSRequestID() {
    return LNSRequestID;
  }

  /**
   * @param id the id to set
   */
  public void setLNSRequestID(int id) {
    this.LNSRequestID = id;
  }

  public int getNSRequestID() {
    return NSRequestID;
  }

  public void setNSRequestID(int NSRequestID) {
    this.NSRequestID = NSRequestID;
  }

  /**
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * @return the recordKey
   */
  public NameRecordKey getRecordKey() {
    return recordKey;
  }

  /**
   * @return the new value
   */
  public ResultValue getUpdateValue() {
    return updateValue;
  }

  public ResultValue getOldValue() {
    return oldValue;
  }

  /**
   * @return the localNameServerId
   */
  public int getLocalNameServerId() {
    return localNameServerId;
  }

  /**
   * @return the nameServerId
   */
  public int getNameServerId() {
    return nameServerId;
  }

  public void setNameServerId(int nameServerId) {
    this.nameServerId = nameServerId;
  }

  /**
   * @return the ttl
   */
  public int getTTL() {
    return ttl;
  }

  /**
   * @return the operation
   */
  public UpdateOperation getOperation() {
    return operation;
  }

  /**
   * @return the operation
   */
  public void setOperation(UpdateOperation operation) {
    this.operation = operation;
  }

  public static void main(String[] args) {
    ResultValue x = new ResultValue();
    x.add("12345678");
//  	
    UpdateAddressPacket up = new UpdateAddressPacket(Packet.PacketType.UPDATE_ADDRESS_NS, 32234234, 123, 2323, "12322323",
            NameRecordKey.EdgeRecord, x, null, UpdateOperation.APPEND_WITH_DUPLICATION, 123, 123,
            GNS.DEFAULT_TTL_SECONDS);

    SizeOf.skipStaticField(true); //java.sizeOf will not compute static fields
    SizeOf.skipFinalField(false); //java.sizeOf will not compute final fields
    SizeOf.skipFlyweightObject(false); //java.sizeOf will not compute well-known flyweight objects
    printSize(up);
    int size = 0;
    try {
      size = up.toJSONObject().toString().getBytes().length;
    } catch (JSONException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    System.out.println("Size = " + size);

  }

  static void printSize(Object paxosReplicaNew){
    System.out.println("Size: " + SizeOf.deepSizeOf(paxosReplicaNew)); //this will print the object size in bytes
  }
}
