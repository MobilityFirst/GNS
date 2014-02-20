package edu.umass.cs.gns.packet;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nameserver.NameRecordKey;
import edu.umass.cs.gns.nameserver.ResultValue;
import edu.umass.cs.gns.nameserver.ValuesMap;
import edu.umass.cs.gns.util.JSONUtils;
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

  public final static String HEADER = "header1";
  public final static String QRECORDKEY = "qrecordkey";
  public final static String QNAME = "qname";
  public final static String TIME_TO_LIVE = "ttlAddress";
  public final static String RECORD_VALUE = "recordValue";

  public final static String ACTIVE_NAME_SERVERS = "Active";

  public final static String LNS_ID = "lnsId";
  /**
   * Packet header *
   */
  private Header header;
  /**
   * Name in the query *
   */
  private String qname;
  /**
   * The key of the value key pair.
   */
  private final NameRecordKey qrecordKey;
  /**
   * Time interval (in seconds) that the resource record may be cached before it should be discarded
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
   * Used by traffic status *
   */
  private int lnsId = -1;

  /**
   **
   * Constructs a packet for querying a name server for name information.
   *
//   * @param header Packet header
   * @param qname Host name in the query
   */
  public DNSPacket(int id, String qname, NameRecordKey key, int sender, String accessor, String signature, String message) {
    super(accessor, signature, message);
    this.header = new Header(id, DNSRecordType.QUERY, DNSRecordType.RCODE_NO_ERROR);
    this.qname = qname;
    this.qrecordKey = key;
    this.lnsId = sender;
  }

  /**
   **
   * Constructs a packet from a JSONObject that represents a DNS packet
   *
   * @param json JSONObject that represents a DNS packet
   * @throws JSONException
   */
  public DNSPacket(JSONObject json) throws JSONException {
    super(json.optString(ACCESSOR, null), json.optString(SIGNATURE, null), json.optString(MESSAGE, null));
    this.header = new Header(json.getJSONObject(HEADER));
    this.qname = json.getString(QNAME);
    this.qrecordKey = NameRecordKey.valueOf(json.getString(QRECORDKEY));

    if (header.getQr() == DNSRecordType.RESPONSE && header.getRcode() != DNSRecordType.RCODE_ERROR) {
      this.ttl = json.getInt(TIME_TO_LIVE);
      this.activeNameServers = JSONUtils.JSONArrayToSetInteger(json.getJSONArray(ACTIVE_NAME_SERVERS));
      if (json.has(RECORD_VALUE)) {
        this.recordValue = new ValuesMap(json.getJSONObject(RECORD_VALUE));
      }
    }
    this.lnsId = json.getInt(LNS_ID);
  }

  /**
   * A shortcut for when you just have a single field you want to return. 
   * 
   * @param id
   * @param name
   * @param key
   * @param fieldValue
   * @param TTL 
   */
  public DNSPacket(int id, String name, NameRecordKey key, ResultValue fieldValue, int TTL, Set<Integer> activeNameServers,
          String accessor, String signature, String message) {
    super(accessor, signature, message);
    this.header = new Header(id, DNSRecordType.RESPONSE, DNSRecordType.RCODE_NO_ERROR);
    this.qname = name;
    this.qrecordKey = key;
    this.recordValue = new ValuesMap();
    this.recordValue.put(key.getName(), fieldValue);
    this.ttl = TTL;
    this.activeNameServers = activeNameServers;

    this.lnsId = -1;
  }

  public DNSPacket(int id, String name, NameRecordKey key, ValuesMap entireRecord, int TTL, Set<Integer> activeNameServers,
          String accessor, String signature, String message) {
    super(accessor, signature, message);
    this.header = new Header(id, DNSRecordType.RESPONSE, DNSRecordType.RCODE_NO_ERROR);
    this.qname = name;
    this.qrecordKey = key;
    this.recordValue = entireRecord;
    this.ttl = TTL;
    this.activeNameServers = activeNameServers;
    this.lnsId = -1;
  }

  /**
   **
   * Converts this packet's query section to a JSONObject
   *
   * @return JSONObject that represents a DNS packet's query section
   * @throws JSONException
   */
  public JSONObject toJSONObjectQuestion() throws JSONException {
    JSONObject json = new JSONObject();
    super.addToJSONObject(json);
    json.put(HEADER, getHeader().toJSONObject());
    json.put(QRECORDKEY, getQrecordKey().getName());
    json.put(QNAME, getQname());
    json.put(LNS_ID, lnsId);

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
    super.addToJSONObject(json);
    json.put(HEADER, getHeader().toJSONObject());
    json.put(QRECORDKEY, getQrecordKey().getName());
    json.put(QNAME, getQname());
    json.put(TIME_TO_LIVE, getTTL());
    json.put(ACTIVE_NAME_SERVERS, new JSONArray(getActiveNameServers()));
    if (recordValue != null)
	      json.put(RECORD_VALUE, recordValue.toJSONObject());

    json.put(LNS_ID, lnsId);
  }

  /**
   *
   * Returns true if the packet is a query, false otherwise
   */
  public boolean isQuery() {
    return getHeader().getQr() == DNSRecordType.QUERY;
  }

  /**
   * Returns true if the packet is a response, false otherwise
   */
  public boolean isResponse() {
    return getHeader().getQr() == DNSRecordType.RESPONSE;
  }

  /**
   **
   * Returns true if the packet contains a response error, false otherwise
   *
   */
  public boolean containsAnyError() {
    return getHeader().getRcode() == DNSRecordType.RCODE_ERROR
            || getHeader().getRcode() == DNSRecordType.RCODE_ERROR_INVALID_ACTIVE_NAMESERVER;
  }

  public boolean containsInvalidActiveNSError() {
    return getHeader().getRcode() == DNSRecordType.RCODE_ERROR_INVALID_ACTIVE_NAMESERVER;
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
      return "DNSPacket{" + "header=" + getHeader() + ", qname=" + getQname() + ", qrecordKey=" + getQrecordKey() + '}';
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
   * @return the qname
   */
  public String getQname() {
    return qname;
  }

  public void setQname(String name) {
    this.qname = name;
  }

  /**
   * @return the qrecordKey
   */
  public NameRecordKey getQrecordKey() {
    return qrecordKey;
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
    this.recordValue.put(qrecordKey.getName(), data);
  }

  public int getLnsId() {
    return lnsId;
  }

  public void setLnsId(int lnsId) {
    this.lnsId = lnsId;
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

}
