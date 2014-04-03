package edu.umass.cs.gns.nsdesign.packet;

import edu.umass.cs.gns.clientsupport.UpdateOperation;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.util.JSONUtils;
import edu.umass.cs.gns.util.NameRecordKey;
import edu.umass.cs.gns.util.ResultValue;
import net.sourceforge.sizeof.SizeOf;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * Implements the packet transmitted from a local name server to update the name to value mapping.
 *
 * A name could have several keys and values, but this packet contains only has option to specify a single key.
 * Therefore, we can only modify one key of the name record using this packet.
 *
 * There are two classes of update operations: (1) updates that modify the keys or values of a name record.
 * (2) Upserts which update an existing record or else create a new name record.
 *
 * Regarding TTL field: If we are using upserts, if the upserts leads to creating a new name record, then TTL value
 * in this packet is used to set ttl for this name record. The value given in the TTL field in not
 * considered in other cases.
 *
 * Future work: I think we should allow clients to change TTLs for a name record after a record is created.
 *
 * A client must set the <code>requestID</code> field correctly to received a response.
 *
 * Once this packet reaches local name server, local name server sets the
 * <code>localNameServerID</code> and <code>LNSRequestID</code> field correctly before forwarding packet
 * to name server.
 *
 * When name server replies to the client, it uses a different packet type: <code>ConfirmUpdateLNSPacket</code>.
 * But it uses fields in this packet in sending the reply.
 *
 * @author Westy
 */
public class UpdateAddressPacket extends BasicPacketWithSignatureInfo {

  private final static String REQUESTID = "reqID";
  private final static String LocalNSREQUESTID = "LNSreqID";
  private final static String NAME = "name";
  private final static String RECORDKEY = "recordkey";
  private final static String NEWVALUE = "newvalue";
  private final static String OLDVALUE = "oldvalue";
  private final static String NAMESERVER_ID = "nsID";
  private final static String LOCAL_NAMESERVER_ID = "lnsID";
  private final static String TTL = "ttl";
  private final static String OPERATION = "operation";
  private final static String ARGUMENT = "argument";
  //
  // We have two ids in here. First one (requestID) is used by the entity making the initial request (often the intercessor).
  // Second (LNSRequestID) is used by the LNS to keep track of it's update records.
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
   * Name (the GUID) *
   */
  private String name;
  /**
   * The key of the value key pair.
   */
  private NameRecordKey recordKey;
  /**
   * Value for updating
   * For simplicity's sake even when we are just updating with a single value it is converted into a
   * list (which is what ResultValue really is).
   */
  private ResultValue updateValue;
  /**
   * The old value to replace in a substitute operation
   *  * For simplicity's sake even when we are just updating with a single value it is converted into a
   * list (which is what ResultValue really is).
   */
  private ResultValue oldValue;
  /**
   * Currently this is used in as the index in the set operation.
   * When used with set the value in updateValue will be the one to set the element to.
   */
  private int argument;
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
  /**
   * Constructs a new UpdateAddressPacket with the given parameters.
   * Used by client support to create a packet to send to the LNS.
   *
   * @param type
   * @param requestID
   * @param name
   * @param recordKey
   * @param newValue
   * @param oldValue
   * @param operation
   * @param localNameServerId
   * @param ttl
   * @param writer
   * @param signature
   * @param message
   */
  public UpdateAddressPacket(Packet.PacketType type, int requestID, String name, NameRecordKey recordKey,
          ResultValue newValue, ResultValue oldValue, int argument, UpdateOperation operation, int localNameServerId, int ttl,
          String writer, String signature, String message) {
    this(type, requestID, -1, name, recordKey, newValue, oldValue, argument, operation, localNameServerId, -1, ttl,
            writer, signature, message);
  }

  /**
   * Constructs a new UpdateAddressPacket with the given parameters.
   * Used by the LNS to create a packet to send to the NS.
   *
   * @param type
   * @param requestID
   * @param LNSRequestID
   * @param name
   * @param recordKey
   * @param newValue
   * @param oldValue
   * @param operation
   * @param localNameServerId
   * @param nameServerId
   * @param ttl
   * @param writer
   * @param signature
   * @param message
   */
  public UpdateAddressPacket(Packet.PacketType type,
          int requestID, int LNSRequestID,
          String name, NameRecordKey recordKey,
          ResultValue newValue,
          ResultValue oldValue,
          int argument,
          UpdateOperation operation,
          int localNameServerId, int nameServerId, int ttl,
          // signature info
          String writer, String signature, String message) {
    // include the signature info
    super(writer, signature, message);
    this.type = type;
    this.requestID = requestID;
    this.LNSRequestID = LNSRequestID;
//    this.NSRequestID = NSRequestID;
    this.name = name;
    this.recordKey = recordKey;
    this.operation = operation;
    this.updateValue = newValue;
    this.oldValue = oldValue;
    this.argument = argument;
    this.localNameServerId = localNameServerId;
    this.nameServerId = nameServerId;
    this.ttl = ttl;
  }

  /**
   * Constructs a new UpdateAddressPacket from a JSONObject.
   *
   * @param json JSONObject that represents UpdatedAddressPacket.
   * @throws org.json.JSONException
   */
  public UpdateAddressPacket(JSONObject json) throws JSONException {
    // include the signature info
    super(json.optString(ACCESSOR, null), json.optString(SIGNATURE, null), json.optString(MESSAGE, null));
    this.type = Packet.getPacketType(json);
    this.requestID = json.getInt(REQUESTID);
    this.LNSRequestID = json.getInt(LocalNSREQUESTID);
//    this.NSRequestID = json.getInt(NameServerREQUESTID);
    this.name = json.getString(NAME);
    this.recordKey = NameRecordKey.valueOf(json.getString(RECORDKEY));
    this.operation = UpdateOperation.valueOf(json.getString(OPERATION));
    this.updateValue = JSONUtils.JSONArrayToResultValue(json.getJSONArray(NEWVALUE));
    this.argument = json.optInt(ARGUMENT, -1);
    this.oldValue = json.has(OLDVALUE) ? JSONUtils.JSONArrayToResultValue(json.getJSONArray(OLDVALUE)) : null;
    this.localNameServerId = json.getInt(LOCAL_NAMESERVER_ID);
    this.nameServerId = json.getInt(NAMESERVER_ID);
    this.ttl = json.getInt(TTL);
  }

  /**
   *
   * Converts a UpdatedAddressPacket to a JSONObject
   *
   * @return JSONObject that represents UpdatedAddressPacket
   * @throws org.json.JSONException
   */
  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    addToJSONObject(json);
    return json;
  }

  /**
   * Add the fields of this UpdatedAddressPacket to an existing JSONObject.
   *
   * @param json
   * @throws org.json.JSONException
   */
  @Override
  public void addToJSONObject(JSONObject json) throws JSONException {
    super.addToJSONObject(json); // include the signature info
    Packet.putPacketType(json, getType());
    json.put(REQUESTID, getRequestID());
    json.put(LocalNSREQUESTID, getLNSRequestID());
//    json.put(NameServerREQUESTID, getNSRequestID());
    json.put(NAME, getName());
    json.put(RECORDKEY, getRecordKey().getName());
    json.put(OPERATION, getOperation().name());
    json.put(NEWVALUE, new JSONArray(getUpdateValue()));
    if (getOldValue() != null) {
      json.put(OLDVALUE, new JSONArray(getOldValue()));
    }
    if (getArgument() != -1) {
      json.put(ARGUMENT, getArgument());
    }
    json.put(LOCAL_NAMESERVER_ID, getLocalNameServerId());
    json.put(NAMESERVER_ID, getNameServerId());
    json.put(TTL, getTTL());
  }

  /**
   * Return the id used the client support for bookkeeping.
   *
   * @return
   */
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

  public int getArgument() {
    return argument;
  }

  /**
   * @return the localNameServerId
   */
  public int getLocalNameServerId() {
    return localNameServerId;
  }

  public void setLocalNameServerId(int localNameServerId) {
    this.localNameServerId = localNameServerId;
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

  //
  // TEST CODE
  //
  public static void main(String[] args) {
    ResultValue x = new ResultValue();
    x.add("12345678");
    //
    UpdateAddressPacket up = new UpdateAddressPacket(Packet.PacketType.UPDATE_ADDRESS_LNS, 32234234, 123, "12322323",
            NameRecordKey.EdgeRecord, x, null, -1, UpdateOperation.APPEND_WITH_DUPLICATION, 123, 123,
            GNS.DEFAULT_TTL_SECONDS, null, null, null);

    SizeOf.skipStaticField(true); //java.sizeOf will not compute static fields
    SizeOf.skipFinalField(false); //java.sizeOf will not compute final fields
    SizeOf.skipFlyweightObject(false); //java.sizeOf will not compute well-known flyweight objects
    printSize(up);
    int size = 0;
    try {
      size = up.toJSONObject().toString().getBytes().length;
    } catch (JSONException e) {
      e.printStackTrace();
    }
    System.out.println("Size = " + size);

  }

  static void printSize(Object object) {
    System.out.println("Size: " + SizeOf.deepSizeOf(object)); //this will print the object size in bytes
  }
}
