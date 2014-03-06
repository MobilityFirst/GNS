package edu.umass.cs.gns.packet;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nameserver.NameRecordKey;
import edu.umass.cs.gns.nameserver.ResultValue;
import edu.umass.cs.gns.nameserver.ValuesMap;
import edu.umass.cs.gns.util.JSONUtils;
import java.util.Random;
import java.util.Set;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 **
 * Packet transmitted between the local name server and a name server. All communications inside of the domain protocol are carried
 * in a single DNS packet. The packet contains the query from a local name server and a response from the name server.
 *
 *
 */
public class DNSPacket extends BasicPacketWithSignatureInfo {

  public final static String HEADER = "dns_header";
  public final static String GUID = "dns_guid";
  public final static String KEY = "dns_key";
  public final static String TIME_TO_LIVE = "ttlAddress";
  public final static String RECORD_VALUE = "recordValue";
  public final static String ACTIVE_NAME_SERVERS = "Active";
  public final static String SENDER_ID = "senderId";
  public final static String RESPONDER = "rspndr";
  /*
   * The header, guid, key and lnsId are called the Question section because
   * they are all that is necessary for a query.
   */
  /**
   * Packet header *
   */
  private Header header;
  /**
   * Name in the query *
   */
  private String guid;
  /**
   * The key of the value key pair.
   */
  private final NameRecordKey key;
  /**
   * This is used by the Nameservers so they know where to send the packet back to. *
   */
  private int senderId = -1;
  /**
   * Time interval (in seconds) that the resource record may be cached before it should be discarded
   */
  /*
   * The ttl, recordValue, and activeNameServers are called the Response section because
   * they are the values that one gets in response to a query.
   */
  private int ttl = GNS.DEFAULT_TTL_SECONDS;
  /**
   * The value that is getting sent back to the client. MOST OF THE TIME THIS WILL HAVE JUST A SINGLE KEY/VALUE, but sometimes we
   * return the entire record. When it's a single key/value the key will be the same as the qrecordKey.
   */
  private ValuesMap recordValue;
  /**
   * A list of active name servers for the name *
   */
  private Set<Integer> activeNameServers;
  /**
   * For response packets this is the node that responded
   */
  private int responder = -1;

  /**
   * Constructs a packet for querying a name server for name information.
   * 
   * @param id
   * @param qname
   * @param key
   * @param sender
   * @param accessor
   * @param signature
   * @param message 
   */
  public DNSPacket(int id, String qname, NameRecordKey key, int sender, String accessor, String signature, String message) {
    super(accessor, signature, message);
    this.header = new Header(id, DNSRecordType.QUERY, NSResponseCode.NO_ERROR);
    this.guid = qname;
    this.key = key;
    this.senderId = sender;
    this.responder = -1;
  }

  /**
   **
   * Constructs a packet from a JSONObject that represents a DNS packet.
   * This packet has both a query and response section.
   * The response section will be empty if this is an error response.
   *
   * @param json JSONObject that represents a DNS packet
   * @throws JSONException
   */
  public DNSPacket(JSONObject json) throws JSONException {
    super(json.optString(ACCESSOR, null), json.optString(SIGNATURE, null), json.optString(MESSAGE, null));
    this.header = new Header(json.getJSONObject(HEADER));
    this.guid = json.getString(GUID);
    this.key = NameRecordKey.valueOf(json.getString(KEY));
    this.senderId = json.getInt(SENDER_ID);
    // read the optional responder if it is there
    this.responder = json.optInt(RESPONDER, -1);

    // These will only be present in non-error response packets
    if (header.isResponse() && !header.isAnyKindOfError()) {
      this.ttl = json.getInt(TIME_TO_LIVE);
      this.activeNameServers = JSONUtils.JSONArrayToSetInteger(json.getJSONArray(ACTIVE_NAME_SERVERS));
      if (json.has(RECORD_VALUE)) {
        this.recordValue = new ValuesMap(json.getJSONObject(RECORD_VALUE));
      }
    }

  }

  /**
   * Creates a DNS packet for when you just have a single field you want to return. 
   * This packet has both a query and response section.
   * 
   * @param id
   * @param name
   * @param key
   * @param fieldValue
   * @param TTL 
   */
  public DNSPacket(int id, String name, NameRecordKey key, ResultValue fieldValue, int TTL, Set<Integer> activeNameServers) {
    this(id, name, key, new ValuesMap(), TTL, activeNameServers);
    // slide that baby in...
    this.recordValue.put(key.getName(), fieldValue);
  }

  /**
   * Creates a DNS packet for when you want to return all of a NameRecord.
   * This packet has both a query and response section.
   * 
   * @param id
   * @param name
   * @param key
   * @param entireRecord
   * @param TTL
   * @param activeNameServers
   * @param accessor
   * @param signature
   * @param message 
   */
  public DNSPacket(int id, String name, NameRecordKey key, ValuesMap entireRecord, int TTL, Set<Integer> activeNameServers) {
    super(); // no sigs for this baby
    this.header = new Header(id, DNSRecordType.RESPONSE, NSResponseCode.NO_ERROR);
    this.guid = name;
    this.key = key;
    this.senderId = -1;
    this.recordValue = entireRecord;
    this.ttl = TTL;
    this.activeNameServers = activeNameServers;
    this.responder = -1;
  }

  /**
   **
   * Converts this packet's query section to a JSONObject.
   * In other words, this packet has just query section and
   * doesn't contain the ttl, activeNameServers or recordValue.
   * But it can have an error value because that value is in the header.
   *
   * @return JSONObject that represents a DNS packet's query section
   * @throws JSONException
   */
  public JSONObject toJSONObjectQuestion() throws JSONException {
    JSONObject json = new JSONObject();
    // include signatures and but not response section
    addToJSONObjectHelper(json, true, false);
    return json;
  }

  /**
   * Converts this packet's query section to a JSONObject and removes all the
   * signature information as well.
   * 
   * @return
   * @throws JSONException 
   */
  public JSONObject toJSONObjectForErrorResponse() throws JSONException {
    JSONObject json = new JSONObject();
    // no signatures and no response section
    addToJSONObjectHelper(json, false, false);
    return json;
  }

  /**
   **
   * Converts this packet's response section to a JSONObject
   *
   * @return JSONObject that represents a DNS packet's response section
   * @throws JSONException
   */
  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    addToJSONObject(json);
    return json;
  }

  @Override
  public void addToJSONObject(JSONObject json) throws JSONException {
    addToJSONObjectHelper(json, true, true);
  }

  public void addToJSONObjectHelper(JSONObject json, boolean includeSignatureSection, boolean includeReponseSection) throws JSONException {
    if (includeSignatureSection) {
      super.addToJSONObject(json);
    }
    // query section
    json.put(HEADER, getHeader().toJSONObject());
    json.put(KEY, getKey().getName());
    json.put(GUID, getGuid());
    json.put(SENDER_ID, senderId);
    // this goes in with query (if it's not empty -1) in case it's an error response and we want to know the reponder
    if (responder != -1) {
      json.put(RESPONDER, responder);
    }
    // response section
    if (includeReponseSection) {
      json.put(TIME_TO_LIVE, getTTL());
      json.put(ACTIVE_NAME_SERVERS, new JSONArray(getActiveNameServers()));
      if (recordValue != null) {
        json.put(RECORD_VALUE, recordValue.toJSONObject());
      }
    }
  }

  /**
   *
   * Returns true if the packet is a query, false otherwise
   */
  public boolean isQuery() {
    return getHeader().isQuery();
  }

  /**
   * Returns true if the packet is a response, false otherwise
   */
  public boolean isResponse() {
    return getHeader().isResponse();
  }

  /**
   **
   * Returns true if the packet contains a response error, false otherwise
   *
   */
  public boolean containsAnyError() {
    return getHeader().isAnyKindOfError();
  }

  public boolean containsInvalidActiveNSError() {
    return getHeader().isInvalidActiveNSError();
  }

  /**
   **
   * Returns the ID for this query from the packet header. Used by the requester to match up replies to outstanding queries
   *
   */
  public int getQueryId() {
    return getHeader().getId();
  }

  @Override
  public String toString() {
    try {
      return isQuery() ? toJSONObjectQuestion().toString() : toJSONObject().toString();
    } catch (JSONException e) {
      return "DNSPacket{" + "header=" + getHeader() + ", guid=" + getGuid() + ", key=" + getKey() + '}';
    }
  }

  /**
   * @return the header
   */
  public Header getHeader() {
    return header;
  }

  /**
   * @param header the header to set
   */
  public void setHeader(Header header) {
    this.header = header;
  }

  /**
   * @return the guid
   */
  public String getGuid() {
    return guid;
  }

  /**
   * @return the qrecordKey
   */
  public NameRecordKey getKey() {
    return key;
  }

  /**
   * @return the ttlAddress
   */
  public int getTTL() {
    return ttl;
  }

  /**
   * @param ttlAddress the ttlAddress to set
   */
  public void setTTL(int ttlAddress) {
    this.ttl = ttlAddress;
  }

  /**
   *
   * @return
   */
  public ValuesMap getRecordValue() {
    return recordValue;
  }

  /**
   *
   * @param recordValue
   */
  public void setRecordValue(ValuesMap recordValue) {
    this.recordValue = recordValue;
  }

  /**
   * @param data the data to set
   */
  public void setSingleReturnValue(ResultValue data) {
    if (this.recordValue == null) {
      this.recordValue = new ValuesMap();
    }
    this.recordValue.put(key.getName(), data);
  }

  public int getSenderId() {
    return senderId;
  }

  public void setSenderId(int lnsId) {
    this.senderId = lnsId;
  }

  /**
   * @return the activeNameServers
   */
  public Set<Integer> getActiveNameServers() {
    return activeNameServers;
  }

  /**
   * @param activeNameServers the activeNameServers to set
   */
  public void setActiveNameServers(Set<Integer> activeNameServers) {
    this.activeNameServers = activeNameServers;
  }

  public int getResponder() {
    return responder;
  }

  public void setResponder(int responder) {
    this.responder = responder;
  }
  
}
