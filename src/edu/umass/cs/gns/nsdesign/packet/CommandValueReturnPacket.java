package edu.umass.cs.gns.nsdesign.packet;

import edu.umass.cs.gns.nsdesign.packet.Packet.PacketType;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Packet format back to the client by local name server in response to a CommandPacket.
 *
 * THIS WILL BE CHANGED WHEN WE GO TO THE NEW FULLY DEEP JSON DATA REPRESENTATION. 
 * IN PARTICULAR THE RETURN VALUE WILL MOST LIKELY BECOME A JSON OBJECT INSTEAD OF A STRING.
 */
public class CommandValueReturnPacket extends BasicPacket {

  private final static String REQUESTID = "reqID";
  private final static String RETURNVALUE = "returnValue";

  /**
   * Identifier of the request.
   */
  private final int requestId;
  private final String returnValue;

  /**
   *
   * @param requestId
   */
  public CommandValueReturnPacket(int requestId, String returnValue) {
    this.setType(PacketType.COMMAND_RETURN_VALUE);
    this.requestId = requestId;
    this.returnValue = returnValue;
  }

  public CommandValueReturnPacket(JSONObject json) throws JSONException {
    this.type = Packet.getPacketType(json);
    this.requestId = json.getInt(REQUESTID);
    this.returnValue = json.getString(RETURNVALUE);
  }

  /**
   * Converts the command object into a JSONObject.
   *
   * @return
   * @throws org.json.JSONException
   */
  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    Packet.putPacketType(json, getType());
    json.put(REQUESTID, this.requestId);
    json.put(RETURNVALUE, returnValue);
    return json;
  }

  public int getRequestId() {
    return requestId;
  }

  public String getReturnValue() {
    return returnValue;
  }

}
