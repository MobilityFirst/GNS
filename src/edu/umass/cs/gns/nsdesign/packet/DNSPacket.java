package edu.umass.cs.gns.nsdesign.packet;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.util.JSONUtils;
import edu.umass.cs.gns.util.NSResponseCode;
import edu.umass.cs.gns.util.NameRecordKey;
import edu.umass.cs.gns.util.ResultValue;
import edu.umass.cs.gns.util.ValuesMap;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import java.util.Set;

/**
 **
 * Packet transmitted between the local name server and a name server. All communications inside of the domain protocol are carried
 * in a single DNS packet. The packet contains the query from a local name server and a response from the name server.
 *
 *
 */
public class DNSPacket extends BasicPacketWithSignatureInfo {

  private final static String HEADER = "dns_header";
  private final static String GUID = "dns_guid";
  private final static String KEY = "dns_key";
  private final static String TIME_TO_LIVE = "ttlAddress";
  private final static String RECORD_VALUE = "recordValue";
  private final static String ACTIVE_NAME_SERVERS = "Active";
  private final static String LNS_ID = "lnsId";
  private final static String SOURCE_ID = "sourceId"; 
  private final static String RESPONDER = "rspndr";
  /**
   * This is the source ID of a packet that should be returned to the intercessor of the LNS.
   * Otherwise the sourceId field contains the number of the NS who made the request.
   */
  public final static int LOCAL_SOURCE_ID = -1;
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
   * This is the Id of the source of the request, -1 means the client is the intercessor of the LNS handling the request. 
   */
  private int sourceId;
  /**
   * This is used by the Nameservers so they know which LNS to send the packet back to. *
   */
  private int lnsId = -1; // will be -1 until set at the LNS
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
   * @param sourceId - the id of the server sending the request, equal to LOCAL_SOURCE_ID means the intercessor of the LNS (the usual case)
   * @param id
   * @param qname
   * @param key
   * @param lnsId
   * @param accessor
   * @param signature
   * @param message
   */
  public DNSPacket(int sourceId, int id, String qname, NameRecordKey key, String accessor, String signature, String message) {
    super(accessor, signature, message);
    this.header = new Header(id, DNSRecordType.QUERY, NSResponseCode.NO_ERROR);
    this.guid = qname;
    this.key = key;
    this.sourceId = sourceId;
    this.lnsId = -1; // this will be -1 until it is set by the handling LNS before sending to an NS
    this.responder = -1;
  }

  /**
   **
   * Constructs a packet from a JSONObject that represents a DNS packet.
   * This packet has both a query and response section.
   * The response section will be empty if this is an error response.
   *
   * @param json JSONObject that represents a DNS packet
   * @throws org.json.JSONException
   */
  public DNSPacket(JSONObject json) throws JSONException {
    super(json.optString(ACCESSOR, null), json.optString(SIGNATURE, null), json.optString(MESSAGE, null));
    this.header = new Header(json.getJSONObject(HEADER));
    this.guid = json.getString(GUID);
    this.key = NameRecordKey.valueOf(json.getString(KEY));
    this.sourceId = json.getInt(SOURCE_ID);
    this.lnsId = json.getInt(LNS_ID);
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
  public DNSPacket(int sourceId, int id, String name, NameRecordKey key, ResultValue fieldValue, int TTL, Set<Integer> activeNameServers) {
    this(sourceId, id, name, key, new ValuesMap(), TTL, activeNameServers);
    // slide that baby in...
    this.recordValue.put(key.getName(), fieldValue);
  }

  /**
   * Creates a DNS packet for when you want to return all of a NameRecord.
   * This packet has both a query and response section.
   *
   * @param sourceId
   * @param id
   * @param name
   * @param key
   * @param entireRecord
   * @param TTL
   * @param activeNameServers
   */
  public DNSPacket(int sourceId, int id, String name, NameRecordKey key, ValuesMap entireRecord, int TTL, Set<Integer> activeNameServers) {
    super(); // no sigs for this baby
    this.header = new Header(id, DNSRecordType.RESPONSE, NSResponseCode.NO_ERROR);
    this.guid = name;
    this.key = key;
    this.lnsId = -1; // we don't care about this once it's heading back to the LNS
    this.sourceId = sourceId;
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
   * @throws org.json.JSONException
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
   * @throws org.json.JSONException
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
   * @throws org.json.JSONException
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
    json.put(SOURCE_ID, sourceId);
    json.put(LNS_ID, lnsId);
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

  public int getLnsId() {
    return lnsId;
  }

  public void setLnsId(int lnsId) {
    this.lnsId = lnsId;
  }

  public int getSourceId() {
    return sourceId;
  }

  public void setSourceId(int sourceId) {
    this.sourceId = sourceId;
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
