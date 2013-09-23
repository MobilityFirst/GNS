package edu.umass.cs.gns.packet;

import edu.umass.cs.gns.nameserver.NameRecordKey;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * A QueryRequestPacket is like a DNS packet without a GUID, but with a value. 
 * The semantics is that we want to look up all 
 * @author westy
 */
public class QueryRequestPacket extends DNSPacket {

  public final static String VALUE = "value";
  private String value;

  public QueryRequestPacket(int id, NameRecordKey recordKey, String value, int sender) {
    super(id, "", recordKey, sender);
    this.value = value;
  }

  public QueryRequestPacket(JSONObject json) throws JSONException {
    super(json);
    this.value = json.getString(VALUE);
  }
  
  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    addToJSONObject(json);
    return json;
  }

  @Override
  public void addToJSONObject(JSONObject json) throws JSONException {
    super.addToJSONObject(json);
    json.put(VALUE, getType());
  }

  public String getValue() {
    return value;
  }
}
