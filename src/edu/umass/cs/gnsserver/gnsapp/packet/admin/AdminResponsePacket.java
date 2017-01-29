
package edu.umass.cs.gnsserver.gnsapp.packet.admin;

import edu.umass.cs.gnscommon.utils.Format;
import edu.umass.cs.gnsserver.gnsapp.packet.BasicPacketWithClientAddress;
import edu.umass.cs.gnsserver.gnsapp.packet.Packet;
import org.json.JSONException;
import org.json.JSONObject;

import java.text.ParseException;
import java.util.Date;


public class AdminResponsePacket extends BasicPacketWithClientAddress {


  public final static String ID = "id";


  public final static String JSON = "json";


  public final static String TIME = "time";
  private int id;
  private Date time;

  private JSONObject jsonObject;


  public AdminResponsePacket(int id, JSONObject jsonObject) {
    this.type = Packet.PacketType.ADMIN_RESPONSE;
    this.id = id;
    this.time = new Date();
    this.jsonObject = jsonObject;
  }


  public AdminResponsePacket(int id) {
    this(id, new JSONObject());
  }


  public AdminResponsePacket(JSONObject json) throws JSONException, ParseException {
    if (Packet.getPacketType(json) != Packet.PacketType.ADMIN_RESPONSE) {
      Exception e = new Exception("AdminResponsePacket: wrong packet type " + Packet.getPacketType(json));
      e.printStackTrace();
      return;
    }

    this.type = Packet.getPacketType(json);
    this.id = json.getInt(ID);
    this.time = Format.parseDateTimeOnlyMilleUTC(json.getString(TIME));
    this.jsonObject = json.getJSONObject(JSON);
  }


  public int getId() {
    return id;
  }


  public Date getTime() {
    return time;
  }


  public JSONObject getJsonObject() {
    return jsonObject;
  }


  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    Packet.putPacketType(json, getType());
    json.put(ID, id);
    json.put(TIME, Format.formatDateTimeOnlyMilleUTC(time));
    json.put(JSON, jsonObject);

    return json;
  }
}
