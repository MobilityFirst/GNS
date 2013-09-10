package edu.umass.cs.gns.packet;

import edu.umass.cs.gns.util.JSONUtils;
import java.util.Set;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class RemoveRecordPacket extends BasicPacket {

  private final static String REQUESTID = "reqID";
  private final static String LNSREQID = "lnreqID";
  private final static String NAME = "name";
  private final static String LOCALNAMESERVERID = "local";
  private final static String PRIMARYNAMESERVERS = "primary";
  /** 
   * Unique identifier used by the entity making the initial request to confirm
   */
  private int requestID;
  /**
   * The ID the LNS uses to for bookeeping
   */
  private int LNSRequestID;
  /**
   * Host/domain/device name *
   */
  private String name;
  /**
   * Id of local nameserver sending this request *
   */
  private int localNameServerID;
  
  /// this will be filled in by the local nameserver
  private Set<Integer> primaryNameServers;
  

  /**
   * ***********************************************************
   * Constructs a new RemoveRecordPacket with the given name and value.
   *
   * @param name Host/domain/device name
   * @param value
   * @param localNameServerID Id of local nameserver sending this request.
   * **********************************************************
   */
  public RemoveRecordPacket(int requestID, String name, int localNameServerID) {
    this.type = Packet.PacketType.REMOVE_RECORD_LNS;
    this.requestID = requestID;
    this.name = name;
    this.localNameServerID = localNameServerID;
  }

  /**
   * ***********************************************************
   * Constructs a new RemoveRecordPacket from a JSONObject
   *
   * @param json JSONObject that represents this packet
   * @throws JSONException **********************************************************
   */
  public RemoveRecordPacket(JSONObject json) throws JSONException {
    if (Packet.getPacketType(json) != Packet.PacketType.REMOVE_RECORD_LNS
            && Packet.getPacketType(json) != Packet.PacketType.REMOVE_RECORD_NS) {
      Exception e = new Exception("AddRecordPacket: wrong packet type " + Packet.getPacketType(json));
      e.printStackTrace();
    }

    this.type = Packet.getPacketType(json);
    this.requestID = json.getInt(REQUESTID);
    this.LNSRequestID = json.getInt(LNSREQID);
    this.name = json.getString(NAME);
    this.localNameServerID = json.getInt(LOCALNAMESERVERID);
    this.primaryNameServers = JSONUtils.JSONArrayToSetInteger(json.getJSONArray(PRIMARYNAMESERVERS));
  }

  /**
   * ***********************************************************
   * Converts AddRecordPacket object to a JSONObject
   *
   * @return JSONObject that represents this packet
   * @throws JSONException **********************************************************
   */
  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    Packet.putPacketType(json, getType());
    json.put(REQUESTID, getRequestID());
    json.put(LNSREQID, getLNSRequestID());
    //json.put(RECORDKEY, getRecordKey().getName());
    json.put(NAME, getName());
    json.put(LOCALNAMESERVERID, getLocalNameServerID());
    json.put(PRIMARYNAMESERVERS, new JSONArray(getPrimaryNameServers()));
    return json;
  }

  public int getRequestID() {
    return requestID;
  }

  public int getLNSRequestID() {
    return LNSRequestID;
  }

  public void setLNSRequestID(int LNSRequestID) {
    this.LNSRequestID = LNSRequestID;
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
//  public NameRecordKey getRecordKey() {
//    return recordKey;
//  }

  /**
   * @return the primaryNameserverId
   */
  public int getLocalNameServerID() {
    return localNameServerID;
  }

  /**
   * @return the primaryNameServers
   */
  public Set<Integer> getPrimaryNameServers() {
    return primaryNameServers;
  }

  /**
   * @param primaryNameServers the primaryNameServers to set
   */
  public void setPrimaryNameServers(Set<Integer> primaryNameServers) {
    this.primaryNameServers = primaryNameServers;
  }
}
