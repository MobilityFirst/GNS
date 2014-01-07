package edu.umass.cs.gns.packet;

import edu.umass.cs.gns.nameserver.NameRecordKey;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * A SelectRequestPacket is like a DNS packet without a GUID, but with a key and value. 
 * The semantics is that we want to look up all the records that have a field named key with the given value.
 * @author westy
 */
public class SelectRequestPacket extends BasicPacket {

  public enum SelectOperation {

    EQUALS,
    NEAR,
    WITHIN,
    QUERY;
  }
  //
  private final static String ID = "id";
  private final static String KEY = "key";
  private final static String VALUE = "value";
  private final static String OTHERVALUE = "otherValue";
  private final static String QUERY = "query";
  private final static String LNSID = "lnsid";
  private final static String LNSQUERYID = "lnsQueryId";
  private final static String OPERATION = "operation";
  private int id;
  private NameRecordKey key;
  private Object value;
  private Object otherValue;
  private String query;
  private int lnsID;
  private int lnsQueryId = -1;
  private SelectOperation operation;

  /**
   * Constructs a new QueryResponsePacket
   * 
   * @param id
   * @param key
   * @param value
   * @param lns 
   */
  public SelectRequestPacket(int id, int lns, SelectOperation operation, NameRecordKey key, Object value, Object otherValue) {
    this.type = Packet.PacketType.SELECT_REQUEST;
    this.id = id;
    this.key = key;
    this.value = value;
    this.otherValue = otherValue;
    this.lnsID = lns;
    this.operation = operation;
    this.query = null;
  }
  
  private SelectRequestPacket(int id, int lns, SelectOperation operation, String query) {
    this.type = Packet.PacketType.SELECT_REQUEST;
    this.id = id;
    this.query = query;
    this.lnsID = lns;
    this.operation = operation;
    this.key = null;
    this.value = null;
    this.otherValue = null;
  }
  
  public static SelectRequestPacket MakeQueryRequest(int id, int lns, String query) {
    return new SelectRequestPacket(id, lns, SelectOperation.QUERY, query);
  }

  /**
   * Constructs new SelectRequestPacket from a JSONObject
   * @param json JSONObject representing this packet
   * @throws JSONException
   */
  public SelectRequestPacket(JSONObject json) throws JSONException {
    if (Packet.getPacketType(json) != Packet.PacketType.SELECT_REQUEST) {
      Exception e = new Exception("QueryRequestPacket: wrong packet type " + Packet.getPacketType(json));
      return;
    }
    this.type = Packet.getPacketType(json);
    this.id = json.getInt(ID);
    this.key = json.has(KEY) ? NameRecordKey.valueOf(json.getString(KEY)) : null;
    this.value = json.optString(VALUE, null);
    this.otherValue = json.optString(OTHERVALUE, null);
    this.query = json.optString(QUERY, null);
    this.lnsID = json.getInt(LNSID);
    this.lnsQueryId = json.getInt(LNSQUERYID);
    this.operation = SelectOperation.valueOf(json.getString(OPERATION));
  }

  /**
   * Converts a SelectRequestPacket to a JSONObject.
   * 
   * @return JSONObject
   * @throws JSONException 
   */
  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    addToJSONObject(json);
    return json;
  }

  private void addToJSONObject(JSONObject json) throws JSONException {
    Packet.putPacketType(json, getType());
    json.put(ID, id);
    if (key != null) {
      json.put(KEY, key.getName());
    }
    if (value != null) {
      json.put(VALUE, value);
    }
    if (otherValue != null) {
      json.put(OTHERVALUE, otherValue);
    }
     if (query != null) {
      json.put(QUERY, query);
    }
    json.put(LNSID, lnsID);
    json.put(LNSQUERYID, lnsQueryId);
    json.put(OPERATION, operation.name());
  }

  public void setLnsQueryId(int lnsQueryId) {
    this.lnsQueryId = lnsQueryId;
  }

  public int getId() {
    return id;
  }

  public NameRecordKey getKey() {
    return key;
  }

  public Object getValue() {
    return value;
  }

  public int getLnsID() {
    return lnsID;
  }

  public int getLnsQueryId() {
    return lnsQueryId;
  }

  public SelectOperation getOperation() {
    return operation;
  }

  public Object getOtherValue() {
    return otherValue;
  }

  public String getQuery() {
    return query;
  }
 
}
