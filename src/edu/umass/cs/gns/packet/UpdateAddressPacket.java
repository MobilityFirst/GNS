package edu.umass.cs.gns.packet;


import edu.umass.cs.gns.client.UpdateOperation;
import edu.umass.cs.gns.nameserver.NameRecordKey;
import edu.umass.cs.gns.util.JSONUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;

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
  private final static String LocalNSREQUESTID = "LSNreqID";
  private final static String NameServerREQUESTID = "NSreqID";
  private final static String NAME = "name";
  private final static String RECORDKEY = "recordkey";
  private final static String NEWVALUE = "newvalue";
  private final static String OLDVALUE = "oldvalue";
  private final static String NAMESERVER_ID = "nsID";
  private final static String LOCAL_NAMESERVER_ID = "lnsID";
//  private final static String TTL = "ttl";
  private final static String OPERATION = "operation";

//  private final static String REQUESTID = "up1";
//  private final static String LocalNSREQUESTID = "up2";
//  private final static String NameServerREQUESTID = "up3";
//  private final static String NAME = "up4";
//  private final static String RECORDKEY = "up5";
//  private final static String NEWVALUE = "up6";
//  private final static String OLDVALUE = "up7";
//  private final static String NAMESERVER_ID = "up8";
//  private final static String LOCAL_NAMESERVER_ID = "up9";
//  //  private final static String TTL = "ttl";
//  private final static String OPERATION = "up10";

//  private final static String PRIMARYNAMESERVERS = "primaryNSs";

//  /**
//   * FROM WHAT I CAN TELL THIS IS ONLY USED TO INIT ConfirmUpdateLNSPacket which never uses it for anything.
//   * REMOVE? - Westy
//   */
//  private int seqNumber;

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
  private ArrayList<String> updateValue;
  /**
   * The old value to replace in a substitute operation *
   */
  private ArrayList<String> oldValue;
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
//  /**
//   * Time to live *
//   */
//  private int ttl;
  
  /// this will be filled in by the local nameserver - used for upsert operations
//  private Set<Integer> primaryNameServers;

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
          ArrayList<String> newValue, ArrayList<String> oldValue, UpdateOperation operation, int localNameServerId) {
    this(type, requestID, -1, -1, name, recordKey, newValue, oldValue, operation,localNameServerId, -1);
  }
  
  public UpdateAddressPacket(Packet.PacketType type,
          int requestID, int LNSRequestID, int NSRequestID,
          String name, NameRecordKey recordKey,
          ArrayList<String> newValue,
          ArrayList<String> oldValue,
          UpdateOperation operation,
          int localNameServerId, int nameServerId) {
    this.type = type;
//    this.seqNumber = sequenceNumber;
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
//    this.ttl = -1;
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
//    this.seqNumber = json.getInt(SEQUENCENUMBER);
    this.requestID = json.getInt(REQUESTID);
    this.LNSRequestID = json.getInt(LocalNSREQUESTID);
    this.NSRequestID = json.getInt(NameServerREQUESTID);
    this.name = json.getString(NAME);
    this.recordKey = NameRecordKey.valueOf(json.getString(RECORDKEY));
    this.operation = UpdateOperation.valueOf(json.getString(OPERATION));
    this.updateValue = JSONUtils.JSONArrayToArrayList(json.getJSONArray(NEWVALUE));
    this.oldValue = json.has(OLDVALUE) ? JSONUtils.JSONArrayToArrayList(json.getJSONArray(OLDVALUE)) : null;
    this.localNameServerId = json.getInt(LOCAL_NAMESERVER_ID);
    this.nameServerId = json.getInt(NAMESERVER_ID);
//    this.ttl = json.getInt(TTL);
//    this.primaryNameServers = JSONUtils.JSONArrayToSetInteger(json.getJSONArray(PRIMARYNAMESERVERS));
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
//    json.put(SEQUENCENUMBER, getSequenceNumber());
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
//    json.put(TTL, getTtl());
//    json.put(PRIMARYNAMESERVERS, new JSONArray(getPrimaryNameServers()));

    return json;
  }

//  /**
//   * @return the id which is used by the request originator for bookkeeping
//   */
//  public int getSequenceNumber() {
//    return seqNumber;
//  }

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
  public ArrayList<String> getUpdateValue() {
    return updateValue;
  }

  public ArrayList<String> getOldValue() {
    return oldValue;
  }
 
  /**
   * @return the localNameServerId
   */
  public int getLocalNameServerId() {
    return localNameServerId;
  }

//    /**
//     * @return the localNameServerId
//     */
//    public void setLocalNameServerId(int localNameServerId) {
//        this.localNameServerId = localNameServerId;
//    }

    /**
   * @return the nameServerId
   */
  public int getNameServerId() {
    return nameServerId;
  }

  public void setNameServerId(int nameServerId) {
    this.nameServerId = nameServerId;
  }

//  /**
//   * @return the ttl
//   */
//  public int getTtl() {
//    return ttl;
//  }
//
//  /**
//   * @param ttl the ttl to set
//   */
//  public void setTtl(int ttl) {
//    this.ttl = ttl;
//  }

//  public Set<Integer> getPrimaryNameServers() {
//    return primaryNameServers;
//  }
//
//  public void setPrimaryNameServers(Set<Integer> primaryNameServers) {
//    this.primaryNameServers = primaryNameServers;
//  }

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
  public static void main(String []args) {
  	ArrayList<String> x = new ArrayList<String>();
  	x.add("12345678");
//  	
  	UpdateAddressPacket up  = new UpdateAddressPacket(Packet.PacketType.UPDATE_ADDRESS_NS, 32234234, 123, 2323, "12322323", NameRecordKey.EdgeRecord, x, null, UpdateOperation.APPEND_WITH_DUPLICATION, 123, 123);

		int size = 0;
		try {
			size = up.toJSONObject().toString().getBytes().length;
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Size = " + size);
		 
  }
}
