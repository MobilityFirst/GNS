package edu.umass.cs.gns.gnsApp.packet;

import edu.umass.cs.gns.gnsApp.clientCommandProcessor.commandSupport.GnsProtocolDefs;
import edu.umass.cs.gns.gnsApp.packet.Packet.PacketType;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.reconfiguration.interfaces.InterfaceReplicableRequest;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * Packet format sent from a client and handled by a local name server.
 *
 */
public class CommandPacket extends BasicPacket implements InterfaceReplicableRequest {

  private final static String CLIENTREQUESTID = "clientreqID";
  private final static String LNSREQUESTID = "LNSreqID";
  private final static String SENDERADDRESS = MessageNIOTransport.SNDR_IP_FIELD;
  private final static String SENDERPORT = MessageNIOTransport.SNDR_PORT_FIELD;
  private final static String COMMAND = "command";

  public final static String BOGUS_SERVICE_NAME = "unknown";

  /**
   * Identifier of the request.
   */
  private final int clientRequestId;
  /**
   * LNS identifier - filled in at the LNS.
   */
  private int LNSRequestId;
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
   * The stop requests needsCoordination() method must return true by default.
   */
  private boolean needsCoordination = true;
  private boolean needsCoordinationExplicitlySet = false;

  /**
   *
   * @param requestId
   * @param command
   */
  public CommandPacket(int requestId, String senderAddress, int senderPort, JSONObject command) {
    this.setType(PacketType.COMMAND);
    this.clientRequestId = requestId;
    this.LNSRequestId = -1; // this will be filled in at the LNS
    this.senderAddress = senderAddress;
    this.senderPort = senderPort;
    this.command = command;
  }

  public CommandPacket(JSONObject json) throws JSONException {
    this.type = Packet.getPacketType(json);
    this.clientRequestId = json.getInt(CLIENTREQUESTID);
    if (json.has(LNSREQUESTID)) {
      this.LNSRequestId = json.getInt(LNSREQUESTID);
    } else {
      this.LNSRequestId = -1;
    }
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
    json.put(CLIENTREQUESTID, this.clientRequestId);
    if (this.LNSRequestId != -1) {
      json.put(LNSREQUESTID, this.LNSRequestId);
    }
    json.put(COMMAND, this.command);
    json.put(SENDERADDRESS, this.senderAddress);
    json.put(SENDERPORT, this.senderPort);
    return json;
  }

  public int getClientRequestId() {
    return clientRequestId;
  }

  public int getLNSRequestId() {
    return LNSRequestId;
  }

  public void setLNSRequestId(int LNSRequestId) {
    this.LNSRequestId = LNSRequestId;
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
        if (command.has(GnsProtocolDefs.GUID)) {
          return command.getString(GnsProtocolDefs.GUID);
        }
        if (command.has(GnsProtocolDefs.NAME)) {
          return command.getString(GnsProtocolDefs.NAME);
        }
      }
    } catch (JSONException e) {
      // Just ignore it
    }
    return BOGUS_SERVICE_NAME;
  }

  public String getCommandName() {
    try {
      if (command != null) {
        if (command.has(GnsProtocolDefs.COMMANDNAME)) {
          return command.getString(GnsProtocolDefs.COMMANDNAME);
        }
      }
    } catch (JSONException e) {
      // Just ignore it
    }
    return "unknown";
  }

  @Override
  public boolean needsCoordination() {
    if (needsCoordinationExplicitlySet) {
      return needsCoordination;
    } else {
      // Cache it.
      needsCoordinationExplicitlySet = true;
      needsCoordination = GnsProtocolDefs.UPDATE_COMMANDS.contains(getCommandName());
      return needsCoordination;
    }
  }

  @Override
  public void setNeedsCoordination(boolean needsCoordination) {
    needsCoordinationExplicitlySet = true;
    this.needsCoordination = needsCoordination;
  }
}
