package edu.umass.cs.gns.packet;

import edu.umass.cs.gns.nameserver.NameRecordKey;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * A QueryRequestPacket is like a DNS packet without a GUID, but with a value. 
 * The semantics is that we want to look up all 
 * @author westy
 */
public class QueryRequestPacket extends BasicPacket {

  public final static String ID = "id";
  public final static String KEY = "key";
  public final static String VALUE = "value";
  public final static String LNS = "lns";
  public final static String LNSQUERYID = "lnsQueryId";
  
  private int id;
  private NameRecordKey key;
  private String value;
  private int lns;
  private int lnsQueryId = -1;

  public QueryRequestPacket(int id, NameRecordKey key, String value, int lns) {
    this.type = Packet.PacketType.QUERY_REQUEST;
    this.id = id;
    this.key = key;
    this.value = value;
    this.lns = lns;
  }
 
  public QueryRequestPacket(JSONObject json) throws JSONException {
    if (Packet.getPacketType(json) != Packet.PacketType.QUERY_REQUEST) {
      Exception e = new Exception("QueryRequestPacket: wrong packet type " + Packet.getPacketType(json));
      return;
    }
    this.id = json.getInt(ID);
    this.key = NameRecordKey.valueOf(json.getString(KEY));
    this.value = json.getString(VALUE);
    this.lns = json.getInt(LNS);
    this.lnsQueryId = json.getInt(LNSQUERYID);
  }
 
  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    addToJSONObject(json);
    return json;
  }

  private void addToJSONObject(JSONObject json) throws JSONException {
    json.put(ID, id);
    json.put(KEY,   key.getName());
    json.put(VALUE, value);
    json.put(LNS, lns);
    json.put(LNSQUERYID, lnsQueryId);
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

  public String getValue() {
    return value;
  }

  public int getLns() {
    return lns;
  }

  public int getLnsQueryId() {
    return lnsQueryId;
  }

}
