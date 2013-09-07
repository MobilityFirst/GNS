package edu.umass.cs.gns.packet;

import org.json.JSONException;
import org.json.JSONObject;

/*************************************************************
 * This class provides the packet transmitted from a local name
 * server to indicate its closest (least ping latency) name 
 * server. 
 * @author Hardeep Uppal
 ************************************************************/
public class NameServerSelectionPacket extends BasicPacket {

  //private final static String RECORDKEY = "recordkey";
  private final static String NAME = "name";
  private final static String VOTE = "vote";
  private final static String UPDATE = "update";
  private final static String NAMESERVER_ID = "nsID";
  private final static String LOCAL_NAMESERVER_ID = "lnsID";
//  private final static String UNIQUE_ID = "uniqueID";
  
  /** Local name server transmitting this packet **/
  private int localnameserverID;
  /** Closest name server id **/
  private int nameserverID;
  /** The key of the value key pair. For GNRS this will be EdgeRecord, CoreRecord or GroupRecord. **/
  //private NameRecordKey recordKey;
  /** Name (service/host/domain or device name) **/
  private String name;
  /** Vote = # of lookup since the last vote **/
  private int vote;
  /** Vote = # of lookup since the last vote **/
  private int update;
  /** Unique ID for this vote message **/
//  private int uniqueID;
  /*************************************************************
   * Constructs a new NSLocationPacket with the given parameters
   * @param name Name (service/host/domain or device name)
   * @param vote # of lookup since the last vote
   * @param nameserverID ID of a name server closest to the
   *        transmitting local name server
   * @param localnameserverID
   ************************************************************/
  public NameServerSelectionPacket(String name,
		  int vote, int update, int nameserverID, int localnameserverID, int uniqueID) {
    this.type = Packet.PacketType.NAMESERVER_SELECTION;
    //this.recordKey = recordKey;
    this.name = name;
    this.vote = vote;
    this.update = update;
    this.nameserverID = nameserverID;
    this.localnameserverID = localnameserverID;
//    this.uniqueID = uniqueID;
  }

  /*************************************************************
   * Constructs a new NSLocationPacket from a JSONObject.
   * @param json JSONObject that represents NSLocaitionPacket
   * @throws JSONException
   ************************************************************/
  public NameServerSelectionPacket(JSONObject json) throws JSONException {
    this.type = Packet.getPacketType(json);
    this.name = json.getString(NAME);
    this.vote = json.getInt(VOTE);
    this.update = json.getInt(UPDATE);
    this.nameserverID = json.getInt(NAMESERVER_ID);
    this.localnameserverID = json.getInt(LOCAL_NAMESERVER_ID);
//    this.uniqueID = json.getInt(UNIQUE_ID);
  }

  /*************************************************************
   * Converts a NSLocationPacket to a JSONObject
   * @return JSONObject that represents NSLocaitionPacket
   * @throws JSONException
   ************************************************************/
  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();

    Packet.putPacketType(json, getType());
    //json.put(RECORDKEY, getRecordKey().getName());
    json.put(NAME, getName());
    json.put(VOTE, getVote());
    json.put(UPDATE, getUpdate());
    json.put(NAMESERVER_ID, getNameserverID());
    json.put(LOCAL_NAMESERVER_ID, getLocalnameserverID());
//    json.put(UNIQUE_ID, uniqueID);
    return json;
  }

  /**
   * @return the localnameserverID
   */
  public int getLocalnameserverID() {
    return localnameserverID;
  }

  /**
   * @return the nameserverID
   */
  public int getNameserverID() {
    return nameserverID;
  }

//  /**
//   * @return the recordKey
//   */
//  public NameRecordKey getRecordKey() {
//    return recordKey;
//  }

  /**
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * @return the vote
   */
  public int getVote() {
    return vote;
  }

  /**
   * @return the vote
   */
  public int getUpdate() {
    return update;
  }
  

  /**
   * @return UniqueID 
   */
  public int getUniqueID() {
    throw new UnsupportedOperationException();
//    return uniqueID;
  }
}
