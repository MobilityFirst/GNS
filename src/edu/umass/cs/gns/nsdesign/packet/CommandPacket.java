package edu.umass.cs.gns.nsdesign.packet;

import edu.umass.cs.gns.clientsupport.Defs;
import edu.umass.cs.gns.nio.JSONNIOTransport;
import edu.umass.cs.gns.nsdesign.packet.Packet.PacketType;
import edu.umass.cs.gns.reconfiguration.InterfaceRequest;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Packet format sent from a client and handled by a local name server.
 *
 */
public class CommandPacket extends BasicPacket implements InterfaceRequest {

  private final static String REQUESTID = "reqID";
  private final static String SENDERADDRESS = JSONNIOTransport.DEFAULT_IP_FIELD;
  private final static String SENDERPORT = JSONNIOTransport.DEFAULT_PORT_FIELD;
  private final static String COMMAND = "command";

  /**
   * Identifier of the request.
   */
  private final int requestId;
  /**
   * The IP address of the sender as a string
   */
  private final String senderAddress;
  /**
   * The TCP port of the sender as an int
   */
  private final int senderPort;
  /**
   * The JSON form of the command. Always includes a COMMANDNAME field.
   * Almost always has a GUID field or NAME (for HRN records) field.
   */
  private final JSONObject command;

  /**
   *
   * @param requestId
   * @param command
   */
  public CommandPacket(int requestId, String senderAddress, int senderPort, JSONObject command) {
    this.setType(PacketType.COMMAND);
    this.requestId = requestId;
    this.senderAddress = senderAddress;
    this.senderPort = senderPort;
    this.command = command;
  }

  public CommandPacket(JSONObject json) throws JSONException {
    this.type = Packet.getPacketType(json);
    this.requestId = json.getInt(REQUESTID);
    this.senderAddress = json.getString(SENDERADDRESS);
    this.senderPort = json.getInt(SENDERPORT);
    this.command = json.getJSONObject(COMMAND);
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
    json.put(SENDERADDRESS, this.senderAddress);
    json.put(SENDERPORT, this.senderPort);
    return json;
  }

  public int getRequestId() {
    return requestId;
  }

  public String getSenderAddress() {
    return senderAddress;
  }

  public int getSenderPort() {
    return senderPort;
  }

  public JSONObject getCommand() {
    return command;
  }

  @Override
  public String getServiceName() {
    try {
      if (command != null) {
        if (command.has(Defs.GUID)) {
          return command.getString(Defs.GUID);
        }
        if (command.has(Defs.NAME)) {
          return command.getString(Defs.NAME);
        }
      }
    } catch (JSONException e) {
      // Just ignore it
    }
    return "Unknown";
  }

}
