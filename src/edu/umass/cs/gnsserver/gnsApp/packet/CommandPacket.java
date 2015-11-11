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
 *  Initial developer(s): Abhigyan Sharma, Westy
 *
 */
package edu.umass.cs.gnsserver.gnsApp.packet;

import edu.umass.cs.gnscommon.GnsProtocol;
import edu.umass.cs.gnsserver.gnsApp.packet.Packet.PacketType;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * Packet format sent from a client and handled by a local name server.
 *
 */
public class CommandPacket extends BasicPacket implements ReplicableRequest {

  private final static String CLIENTREQUESTID = "clientreqID";
  private final static String LNSREQUESTID = "LNSreqID";
  private final static String SENDERADDRESS = MessageNIOTransport.SNDR_IP_FIELD;
  private final static String SENDERPORT = MessageNIOTransport.SNDR_PORT_FIELD;
  private final static String COMMAND = "command";

  /** bogus service name */
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
   * Create a CommandPacket instance.
   * 
   * @param requestId
   * @param senderAddress
   * @param command
   * @param senderPort
   */
  public CommandPacket(int requestId, String senderAddress, int senderPort, JSONObject command) {
    this.setType(PacketType.COMMAND);
    this.clientRequestId = requestId;
    this.LNSRequestId = -1; // this will be filled in at the LNS
    this.senderAddress = senderAddress;
    this.senderPort = senderPort;
    this.command = command;
  }

  /**
   * Create a CommandPacket instance.
   *
   * @param json
   * @throws JSONException
   */
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
   * @return a JSONObject
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

  /**
   * Return the client request id.
   * 
   * @return the client request id
   */
  public int getClientRequestId() {
    return clientRequestId;
  }

  /**
   * Return the LNS request id.
   * 
   * @return the LNS request id
   */
  public int getLNSRequestId() {
    return LNSRequestId;
  }

  /**
   * Set the LNS request id.
   * 
   * @param LNSRequestId
   */
  public void setLNSRequestId(int LNSRequestId) {
    this.LNSRequestId = LNSRequestId;
  }

  /**
   * Return the sender address.
   * 
   * @return a string
   */
  public String getSenderAddress() {
    return senderAddress;
  }

  /**
   * Return the sender port.
   * 
   * @return the sender port
   */
  public int getSenderPort() {
    return senderPort;
  }

  /**
   * Return the command.
   * 
   * @return the command
   */
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
}
