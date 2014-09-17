package edu.umass.cs.gns.nsdesign.packet.admin;

import edu.umass.cs.gns.nsdesign.nodeconfig.NodeId;
import edu.umass.cs.gns.nsdesign.packet.BasicPacket;
import edu.umass.cs.gns.nsdesign.packet.Packet;
import edu.umass.cs.gns.util.Format;
import org.json.JSONException;
import org.json.JSONObject;

import java.text.ParseException;
import java.util.Date;

/**
 * This class implements a packet that contains status information
 * 
 * @author Westy
 */
public class StatusPacket extends BasicPacket {

  public final static String ID = "id";
  public final static String JSON = "json";
  public final static String TIME = "time";
  private NodeId<String> id;
  private Date time;
  /** JOSNObject where the results are kept **/
  private JSONObject jsonObject;

  /**
   * Constructs a new status packet with the given JSONObject
   * @param id
   * @param jsonObject
   */
  public StatusPacket(NodeId<String> id, JSONObject jsonObject) {
    this.type = Packet.PacketType.STATUS;
    this.id = id;
    this.time = new Date();
    this.jsonObject = jsonObject;
  }

  /**
   * Constructs a new empty status packet
   *
   * @param id
   */
  public StatusPacket(NodeId<String> id) {
    this(id, new JSONObject());
  }

  /**
   * Constructs new StatusPacket from a JSONObject
   * @param json JSONObject representing this packet
   * @throws org.json.JSONException
   */
  public StatusPacket(JSONObject json) throws JSONException, ParseException {
    if (Packet.getPacketType(json) != Packet.PacketType.STATUS) {
      Exception e = new Exception("StatusPacket: wrong packet type " + Packet.getPacketType(json));
      e.printStackTrace();
      return;
    }

    this.type = Packet.getPacketType(json);
    this.id = new NodeId<String>(json.getString(ID));
    this.time = Format.parseDateTimeOnlyMilleUTC(json.getString(TIME));
    this.jsonObject = json.getJSONObject(JSON);
  }

  public NodeId<String> getId() {
    return id;
  }

  public Date getTime() {
    return time;
  }

  public JSONObject getJsonObject() {
    return jsonObject;
  }

  /**
   * Converts a StatusPacket to a JSONObject.
   * @return JSONObject representing this packet.
   * @throws org.json.JSONException
   */
  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    Packet.putPacketType(json, getType());
    json.put(ID, id.get());
    json.put(TIME, Format.formatDateTimeOnlyMilleUTC(time));
    json.put(JSON, jsonObject);

    return json;
  }
}
