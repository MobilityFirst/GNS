package edu.umass.cs.gnrs.packet;

import edu.umass.cs.gnrs.client.Intercessor;
import edu.umass.cs.gnrs.nameserver.NameRecordKey;
import edu.umass.cs.gnrs.packet.Packet.PacketType;
import edu.umass.cs.gnrs.packet.UpdateOperation;
import edu.umass.cs.gnrs.util.JSONUtils;
import java.util.ArrayList;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * ***********************************************************
 * This class implements the packet that confirms update, add and remove transactions. The packet is transmitted from an
 * active name server to a local name server that had originally sent the address update. Also used sent back to the
 * original client ({@link Intercessor} is one example).
 *
 * @author Westy *******************************************************
 */
public class ConfirmUpdateLNSPacket extends BasicPacket {

  private final static String SEQUENCENUMBER = "seqnum";
  private final static String REQUESTID = "reqid";
  private final static String LNSREQUESTID = "lnreqsid";
  private final static String SUCCESS = "success";
  private final static String NAME = "name";
  private final static String RECORDKEY = "recordkey";
  private final static String NUM_NAMESERVERS_UPDATES = "numNSupdates";
  private final static String NAMESERVER_ID = "nsID";
  private final static String LOCAL_NAMESERVER_ID = "lnsID";
  private final static String NEWVALUE = "newvalue";
  private final static String OPERATION = "operation";
  /**
   * unique identifier used in confirm protocol
   */
  private int sequenceNumber;
  /**
   * Unique identifier used by the entity making the initial request to confirm
   */
  private int requestID;
  /**
   * The ID the LNS uses to for bookkeeping
   */
  private int LNSRequestID;
  /**
   * indicates success or failure of operation
   */
  private boolean success;
  /**
   * Name (service/host/domain or device name) *
   */
  private String name;
  /**
   * The key of the value key pair. For GNRS this will be EdgeRecord, CoreRecord or GroupRecord. *
   */
  private NameRecordKey recordKey;
  /**
   * Number of name servers updated *
   */
  private int numNameServersUpdated;
  /**
   * Name server transmitting this packet *
   */
  private int nameServerId;
  /**
   * Local name server updating address *
   */
  private int localNameServerId;
  private ArrayList<String> updateValue;
  /**
   * The operation to perform *
   */
  private UpdateOperation operation;

  /**
   * Local name server transmitting this packet *
   */
  /**
   * ***********************************************************
   * Constructs a new ConfirmUpdatePacket with the given parameters.
   *
   * @param type Type of this packet
   * @param name Name (service/host/domain or device name)
   * @param nameServerID Id of local or name server **********************************************************
   */
  public ConfirmUpdateLNSPacket(PacketType type, int sequenceNumber, int requestID, int LNSRequestID, String name, NameRecordKey recordKey,
          ArrayList<String> updateValue, UpdateOperation operation,
          boolean success,
          int nameServerId, int localNameServer, int numNameServersUpdated) {
    this.type = type;
    this.success = success;
    this.sequenceNumber = sequenceNumber;
    this.requestID = requestID;
    this.LNSRequestID = LNSRequestID;
    this.name = name;
    this.recordKey = recordKey;
    this.nameServerId = nameServerId;
    this.localNameServerId = localNameServer;
    this.numNameServersUpdated = numNameServersUpdated;
    this.updateValue = updateValue;
    this.operation = operation;
  }

  public static ConfirmUpdateLNSPacket createFailPacket(UpdateAddressPacket updatePacket, int nameServerId) {
    return new ConfirmUpdateLNSPacket(PacketType.CONFIRM_UPDATE_LNS, updatePacket.getSequenceNumber(),
            updatePacket.getRequestID(), updatePacket.getLNSRequestID(), updatePacket.getName(), updatePacket.getRecordKey(),
            updatePacket.getUpdateValue(), updatePacket.getOperation(),
            false,
            nameServerId, updatePacket.getLocalNameServerId(), 0);
  }

  public static ConfirmUpdateLNSPacket createSuccessPacket(UpdateAddressPacket updatePacket, int nameServerId, int numNameServersUpdated) {
    return new ConfirmUpdateLNSPacket(PacketType.CONFIRM_UPDATE_LNS, updatePacket.getSequenceNumber(),
            updatePacket.getRequestID(), updatePacket.getLNSRequestID(), updatePacket.getName(), updatePacket.getRecordKey(),
            updatePacket.getUpdateValue(), updatePacket.getOperation(),
            true,
            nameServerId, updatePacket.getLocalNameServerId(), numNameServersUpdated);
  }

    public void convertToFailPacket() {
        this.success = false;
        this.numNameServersUpdated = 0;

    }
  public ConfirmUpdateLNSPacket(int nameServerID, boolean success, AddRecordPacket packet) {
    this(Packet.PacketType.CONFIRM_ADD_LNS, 0, packet.getRequestID(), packet.getLNSRequestID(),
            packet.getName(), packet.getRecordKey(),
            packet.getValue(), null,
            success, nameServerID, packet.getLocalNameServerID(), 0);
  }

  public ConfirmUpdateLNSPacket(int nameServerID, boolean success, RemoveRecordPacket packet) {
    this(Packet.PacketType.CONFIRM_REMOVE_LNS, 0, packet.getRequestID(), packet.getLNSRequestID(),
            packet.getName(), 
            new NameRecordKey(""),
            //packet.getRecordKey(),
            null, null,
            success, nameServerID, packet.getLocalNameServerID(), 0);
  }

  /**
   * ***********************************************************
   * Constructs a new ConfirmUpdatePacket from a JSONObject.
   *
   * @param json JSONObject that represents ConfirmUpdatePacket.
   * @throws JSONException **********************************************************
   */
  public ConfirmUpdateLNSPacket(JSONObject json) throws JSONException {
    this.type = Packet.getPacketType(json);
    this.sequenceNumber = json.getInt(SEQUENCENUMBER);
    this.requestID = json.getInt(REQUESTID);
    this.LNSRequestID = json.getInt(LNSREQUESTID);
    this.success = json.getBoolean(SUCCESS);
    this.name = json.getString(NAME);
    this.recordKey = NameRecordKey.valueOf(json.getString(RECORDKEY));
    this.nameServerId = json.getInt(NAMESERVER_ID);
    this.localNameServerId = json.getInt(LOCAL_NAMESERVER_ID);
    this.numNameServersUpdated = json.getInt(NUM_NAMESERVERS_UPDATES);
    this.operation = json.has(OPERATION) ? UpdateOperation.valueOf(json.getString(OPERATION)) : null;
    this.updateValue = json.has(NEWVALUE) ? JSONUtils.JSONArrayToArrayList(json.getJSONArray(NEWVALUE)) : null;
  }

  /**
   * ***********************************************************
   * Converts a ConfirmUpdatePacket to a JSONObject
   *
   * @return JSONObject that represents ConfirmUpdatePacket
   * @throws JSONException **********************************************************
   */
  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    Packet.putPacketType(json, getType());
    json.put(SEQUENCENUMBER, getSequenceNumber());
    json.put(REQUESTID, getRequestID());
    json.put(LNSREQUESTID, getLNSRequestID());
    json.put(SUCCESS, isSuccess());
    json.put(NAME, getName());
    json.put(RECORDKEY, getRecordKey().getName());
    json.put(NAMESERVER_ID, getNameServerId());
    json.put(LOCAL_NAMESERVER_ID, getLocalNameServerId());
    json.put(NUM_NAMESERVERS_UPDATES, getNumNameServersUpdated());
    if (getOperation() != null) {
      json.put(OPERATION, getOperation().name());
    }
    if (getUpdateValue() != null) {
      json.put(NEWVALUE, new JSONArray(getUpdateValue()));
    }

    return json;
  }

  /**
   * @return the id
   */
  public int getSequenceNumber() {
    return sequenceNumber;
  }

  public int getRequestID() {
    return requestID;
  }

  public int getLNSRequestID() {
    return LNSRequestID;
  }

  public boolean isSuccess() {
    return success;
  }

  public void setSuccess(boolean success) {
    this.success = success;
  }

  /**
   * @return the recordKey
   */
  public NameRecordKey getRecordKey() {
    return recordKey;
  }

  /**
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * @return the nameServerId
   */
  public int getNameServerId() {
    return nameServerId;
  }

  /**
   * @return the localNameServerId
   */
  public int getLocalNameServerId() {
    return localNameServerId;
  }

  public int getNumNameServersUpdated() {
    return numNameServersUpdated;
  }

  public void setNumNameServersUpdated(int numNameServersUpdated) {
    this.numNameServersUpdated = numNameServersUpdated;
  }

  public ArrayList<String> getUpdateValue() {
    return updateValue;
  }

  public UpdateOperation getOperation() {
    return operation;
  }
}
