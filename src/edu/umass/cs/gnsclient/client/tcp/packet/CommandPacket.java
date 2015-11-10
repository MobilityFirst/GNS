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
import edu.umass.cs.nio.MessageNIOTransport;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Packet format sent from a client and handled by a local name server.
 *
 */
public class CommandPacket extends BasicPacket {

  private final static String REQUESTID = "clientreqID"; // needs to be the same as the field in the server
  private final static String SENDERIP = MessageNIOTransport.SNDR_IP_FIELD; // GNS knows about this field
  private final static String SENDERPORT = MessageNIOTransport.SNDR_PORT_FIELD; // GNS knows about this field
  private final static String COMMAND = "command";

  /**
   * Identifier of the request.
   */
  private int requestId; // not final because of testing requirement
  /**
   * The IP address of the sender as a string. 
   * Will be inserted by the receiver.
   */
  private final String senderIP;
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
   * Creates a CommandPacket.
   * 
   * @param requestId
   * @param senderHost
   * @param senderPort
   * @param command
   */
  public CommandPacket(int requestId, String senderHost, int senderPort, JSONObject command) {
    this.setType(PacketType.COMMAND);
    this.requestId = requestId;
    this.senderIP = senderHost;
    this.senderPort = senderPort;
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
    this.requestId = json.getInt(REQUESTID);
    this.senderIP = json.optString(SENDERIP, null);
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
    json.put(REQUESTID, this.requestId);
    json.put(COMMAND, this.command);
    if (senderIP != null) {
      json.put(SENDERIP, this.senderIP);
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
    return senderIP;
  }

  public int getSenderPort() {
    return senderPort;
  }

  public JSONObject getCommand() {
    return command;
  }

}
