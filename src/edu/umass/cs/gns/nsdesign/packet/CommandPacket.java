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
  private static final String COMMAND = "command";
  private final static String RESPONSECODE = "code";
  private final static String ERRORSTRING = "error";

  /**
   * Identifier of the request.
   */
  private final int requestId;

  /**
   * ID of the group between new set of active replicas.
   */
  private final JSONObject command;
  private final NSResponseCode responseCode;
  private final String errorMessage;

  /**
   *
   * @param requestId
   * @param command
   */
  public CommandPacket(int requestId, JSONObject command) {
    this.setType(PacketType.COMMAND);
    this.requestId = requestId;
    this.command = command;
    this.responseCode = null;
    this.errorMessage = null;

  }

  /**
   *
   * @param requestId
   * @param command
   */
  public CommandPacket(int requestId, JSONObject command, NSResponseCode responseCode, String errorMessage) {
    this.setType(PacketType.COMMAND);
    this.requestId = requestId;
    this.command = command;
    this.responseCode = responseCode;
    this.errorMessage = errorMessage;
  }

  public CommandPacket(JSONObject json) throws JSONException {
    this.type = Packet.getPacketType(json);
    this.requestId = json.getInt(REQUESTID);
    this.command = json.getJSONObject(COMMAND);
    this.responseCode = json.has(RESPONSECODE) ? NSResponseCode.getResponseCode(json.getInt(RESPONSECODE)) : null;
    this.errorMessage = json.has(ERRORSTRING) ? json.optString(ERRORSTRING, null) : null;
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
    json.put(COMMAND, this.command);
    if (responseCode != null) {
      json.put(RESPONSECODE, responseCode.name());
    }
    if (errorMessage != null) {
      json.put(ERRORSTRING, errorMessage);
    }
    return json;
  }

  public int getRequestId() {
    return requestId;
  }

  public JSONObject getCommand() {
    return command;
  }

  public NSResponseCode getResponseCode() {
    return responseCode;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

}
