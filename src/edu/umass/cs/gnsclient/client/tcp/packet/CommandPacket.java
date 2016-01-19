/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *  Initial developer(s): Westy, Emmanuel Cecchet
 *
 */
package edu.umass.cs.gnsclient.client.tcp.packet;

import edu.umass.cs.gnsclient.client.tcp.packet.Packet.PacketType;
import edu.umass.cs.gnscommon.GnsProtocol;
import static edu.umass.cs.gnsserver.gnsApp.packet.CommandPacket.BOGUS_SERVICE_NAME;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Packet format sent from a client LNS or AR.
 *
 */
public class CommandPacket extends BasicPacket implements ReplicableRequest {

  private final static String CLIENTREQUESTID = "clientreqID"; // needs to be the same as the field in the server
  private final static String LNSREQUESTID = "LNSreqID"; // only needed by LNS and server
  private final static String SENDERADDRESS = MessageNIOTransport.SNDR_IP_FIELD; // GNS knows about this field
  private final static String SENDERPORT = MessageNIOTransport.SNDR_PORT_FIELD; // GNS knows about this field
  private final static String COMMAND = "command";

  /**
   * Identifier of the request.
   */
  private int requestId; // not final because of testing requirement
  /**
   * LNS identifier - filled in at the LNS.
   */
  private int LNSRequestId;
  /**
   * The IP address of the sender as a string.
   * Will be inserted by the receiver.
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
   * Creates a CommandPacket.
   *
   * @param requestId
   * @param senderAddress
   * @param senderPort
   * @param command
   */
  public CommandPacket(int requestId, String senderAddress, int senderPort, JSONObject command) {
    this.setType(PacketType.COMMAND);
    this.requestId = requestId;
    this.senderAddress = senderAddress;
    this.senderPort = senderPort;
    this.command = command;
  }

  /**
   * Creates a command packet with a null host and -1 port which will be
   * filled in when the packet is sent out.
   *
   * @param requestId
   * @param command
   */
  public CommandPacket(int requestId, JSONObject command) {
    this.setType(PacketType.COMMAND);
    this.requestId = requestId;
    this.senderAddress = null;
    this.senderPort = -1;
    this.command = command;
  }

  /**
   * Creates a CommandPacket from a JSONObject.
   *
   * @param json
   * @throws JSONException
   */
  public CommandPacket(JSONObject json) throws JSONException {
    this.type = Packet.getPacketType(json);
    this.requestId = json.getInt(CLIENTREQUESTID);
    if (json.has(LNSREQUESTID)) {
      this.LNSRequestId = json.getInt(LNSREQUESTID);
    } else {
      this.LNSRequestId = -1;
    }
    this.senderAddress = json.optString(SENDERADDRESS, null);
    this.senderPort = json.optInt(SENDERPORT, -1);
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
    json.put(CLIENTREQUESTID, this.requestId);
    if (this.LNSRequestId != -1) {
      json.put(LNSREQUESTID, this.LNSRequestId);
    }
    json.put(COMMAND, this.command);
    if (senderAddress != null) {
      json.put(SENDERADDRESS, this.senderAddress);
    }
    if (senderPort != -1) {
      json.put(SENDERPORT, this.senderPort);
    }
    return json;
  }

  public int getRequestId() {
    return requestId;
  }

  // only for testing
  public void setRequestId(int requestId) {
    this.requestId = requestId;
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
        if (command.has(GnsProtocol.GUID)) {
          return command.getString(GnsProtocol.GUID);
        }
        if (command.has(GnsProtocol.NAME)) {
          return command.getString(GnsProtocol.NAME);
        }
      }
    } catch (JSONException e) {
      // Just ignore it
    }
    return BOGUS_SERVICE_NAME;
  }

  /**
   * Return the command name.
   *
   * @return the command name
   */
  public String getCommandName() {
    try {
      if (command != null) {
        if (command.has(GnsProtocol.COMMANDNAME)) {
          return command.getString(GnsProtocol.COMMANDNAME);
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
      needsCoordination = GnsProtocol.UPDATE_COMMANDS.contains(getCommandName());
      return needsCoordination;
    }
  }

  @Override
  public void setNeedsCoordination(boolean needsCoordination) {
    needsCoordinationExplicitlySet = true;
    this.needsCoordination = needsCoordination;
  }

  @Override
  public void addToJSONObject(JSONObject json) throws JSONException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

}
