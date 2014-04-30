package edu.umass.cs.gns.nsdesign.packet;

import edu.umass.cs.gns.nsdesign.packet.Packet.PacketType;
import edu.umass.cs.gns.util.NSResponseCode;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * This packet is used to send commands from a Local Name Server to a Name Server
 * as well as send responses back to the LNS.
 */
public class CommandPacket extends BasicPacket {

  private final static String REQUESTID = "reqID";
  private final static String COMMAND = "command";
  private final static String RETURNVALUE = "returnValue";
  private final static String LNSID = "lnsid";

  /**
   * Identifier of the request.
   */
  private final int requestId;

  /**
   * ID of the group between new set of active replicas.
   */
  private final JSONObject command;
  private String returnValue;
  private final int lnsID; // the local name server handling this request
  

  /**
   *
   * @param requestId
   * @param command
   */
  public CommandPacket(int requestId, int lns, JSONObject command) {
    this.setType(PacketType.COMMAND);
    this.requestId = requestId;
    this.lnsID = lns;
    this.command = command;
    this.returnValue = null; // only set when the packet is ready to send back

  }

  public CommandPacket(JSONObject json) throws JSONException {
    this.type = Packet.getPacketType(json);
    this.requestId = json.getInt(REQUESTID);
    this.lnsID = json.getInt(LNSID);
    this.command = json.getJSONObject(COMMAND);
    this.returnValue = json.has(RETURNVALUE) ? json.optString(RETURNVALUE, null) : null;
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
    json.put(LNSID, lnsID);
    json.put(COMMAND, this.command);
    if (returnValue != null) {
      json.put(RETURNVALUE, returnValue);
    }
    return json;
  }

  public int getRequestId() {
    return requestId;
  }

  public static String getLNSID() {
    return LNSID;
  }

  public int getLnsID() {
    return lnsID;
  }

  public JSONObject getCommand() {
    return command;
  }

  public String getReturnValue() {
    return returnValue;
  }

  public void setReturnValue(String returnValue) {
    this.returnValue = returnValue;
  }

}
