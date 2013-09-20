package edu.umass.cs.gns.packet;

import edu.umass.cs.gns.nameserver.ResultValue;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nameserver.NameRecordKey;
import edu.umass.cs.gns.nameserver.ValuesMap;
import java.text.ParseException;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 **
 * Packet transmitted between the local name server and a name server. All communications inside of the domain protocol are carried
 * in a single DNS packet. The packet contains the query from a local name server and a response from the name server.
 *
 *
 */
public class QueryRequestPacket extends BasicPacket {

  public final static String ID = "id";
  public final static String KEY = "key";
  public final static String VALUE = "value";
  private int id;
  private String key;
  private String value;

  /**
   * Constructs a QueryRequestPacket packet.
   * 
   * @param id
   * @param key
   * @param value 
   */
  public QueryRequestPacket(int id, String key, String value) {
    this.id = id;
    this.key = key;
    this.value = value;
  }

  /**
   **
   * Constructs a QueryRequestPacket packet from a JSONObject that represents a QueryRequestPacket packet
   *
   * @param json JSONObject that represents a DNS packet
   * @throws JSONException
   */
  public QueryRequestPacket(JSONObject json) throws JSONException {
    if (Packet.getPacketType(json) != Packet.PacketType.QUERY_REQUEST) {
      Exception e = new Exception("StatusPacket: wrong packet type " + Packet.getPacketType(json));
      e.printStackTrace();
      return;
    }
    this.type = Packet.getPacketType(json);
    this.id = json.getInt(ID);
    this.key = json.getString(KEY);
    this.value = json.getString(VALUE);
  }

  /*************************************************************
   * Converts a ActiveNSUpdatePacket to a JSONObject.
   * @return JSONObject representing this packet.
   * @throws JSONException
   ************************************************************/
  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    Packet.putPacketType(json, getType());
    json.put(ID, getId());
    json.put(KEY, getKey());
    json.put(VALUE, getValue());

    return json;
  }

  public int getId() {
    return id;
  }

  public String getKey() {
    return key;
  }

  public String getValue() {
    return value;
  }
}
