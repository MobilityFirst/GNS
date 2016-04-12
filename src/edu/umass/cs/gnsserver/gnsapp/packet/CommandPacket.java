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
package edu.umass.cs.gnsserver.gnsapp.packet;

import java.net.InetSocketAddress;

import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gnscommon.GnsProtocol;
import edu.umass.cs.gnsserver.gnsapp.packet.Packet.PacketType;
import static edu.umass.cs.gnsserver.gnsapp.packet.Packet.getPacketType;
import static edu.umass.cs.gnsserver.gnsapp.packet.Packet.putPacketType;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * @author westy, arun
 *
 * Packet format sent from a client and handled by a local name server.
 *
 */
public class CommandPacket extends BasicPacketWithClientAddress implements ClientRequest, ReplicableRequest {

  private final static String CLIENTREQUESTID = "clientreqID";
  private final static String LNSREQUESTID = "LNSreqID";
  private final static String SENDERADDRESS = MessageNIOTransport.SNDR_IP_FIELD;
  private final static String SENDERPORT = MessageNIOTransport.SNDR_PORT_FIELD;
  private final static String COMMAND = "command";

  /**
   * bogus service name
   */
  public final static String BOGUS_SERVICE_NAME = "unknown";

  /**
   * Identifier of the request on the client.
   */
  private long clientRequestId;
  /**
   * LNS identifier - filled in at the LNS.
   */
  private long LNSRequestId;
  /**
   * The IP address of the sender as a string
   */
  private final String senderAddress;
  /**
   * The TCP port of the sender as an int
   */
  private final int senderPort;

  // arun: Need this for correct receiver messaging 
  private final InetSocketAddress myListeningAddress;
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

  private int retransmissions = 0;

  /**
   * Create a CommandPacket instance.
   *
   * @param requestId
   * @param senderAddress
   * @param command
   * @param senderPort
   * @param myListeningAddress
   */
  public CommandPacket(long requestId, String senderAddress, int senderPort, JSONObject command) {
    this.setType(PacketType.COMMAND);
    this.clientRequestId = requestId;
    this.senderAddress = senderAddress;
    this.senderPort = senderPort;
    this.command = command;

    this.LNSRequestId = -1; // this will be filled in at the LNS
    this.myListeningAddress = null;
  }

  /**
   * Creates a command packet with a null host and -1 port which will be
   * filled in when the packet is sent out.
   *
   * @param requestId
   * @param command
   */
  public CommandPacket(long requestId, JSONObject command) {
    this(requestId, null, -1, command);
  }

  /**
   * Creates a CommandPacket instance from a JSONObject.
   *
   * @param json
   * @throws JSONException
   */
  public CommandPacket(JSONObject json) throws JSONException {
    this.type = getPacketType(json);
    this.clientRequestId = json.getLong(CLIENTREQUESTID);
    if (json.has(LNSREQUESTID)) {
      this.LNSRequestId = json.getLong(LNSREQUESTID);
    } else {
      this.LNSRequestId = json.getLong(CLIENTREQUESTID);//-1;
    }
    this.senderAddress = json.optString(SENDERADDRESS, null);
    this.senderPort = json.optInt(SENDERPORT, -1);
    this.command = json.getJSONObject(COMMAND);

    this.myListeningAddress = MessageNIOTransport.getReceiverAddress(command);

  }

  public InetSocketAddress getMyListeningAddress() {
    return this.myListeningAddress;
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
    putPacketType(json, getType());
    json.put(CLIENTREQUESTID, this.clientRequestId);
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
    if (this.myListeningAddress != null)
    	// do nothing
    	;
    return json;
  }

  /**
   * Return the client request id.
   *
   * @return the client request id
   */
  public long getClientRequestId() {
    return clientRequestId;
  }

  /**
   * Return the client request id as a long.
   *
   * @return the client request id
   */
  @Override
  public long getRequestID() {
    return clientRequestId;
  }

  /**
   * For ClientRequest.
   *
   * @return
   */
  @Override
  public ClientRequest getResponse() {
    return this.response;
  }

  // only for testing
  public void setClientRequestId(long requestId) {
    this.clientRequestId = requestId;
  }

  /**
   * Return the LNS request id.
   *
   * @return the LNS request id
   */
  public long getLNSRequestId() {
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
   * @return {@code this}
   */
  public CommandPacket incrRetransmissions() {
    this.retransmissions++;
    return this;
  }

  /**
   * @return Number of retransmissions.
   */
  public int getRetransmissions() {
    return this.retransmissions;
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

  /**
   * The service name should be the name of the GUID that is being written to
   * or read, not the account GUID. To address the HRN/GUID ambiguity, you should
   * either (1) issue each as separate requests from the client; or (2) retransmit
   * a request until the replica it happens to go to has caught up; or (3) accept
   * that it is normal behavior for a read immediately following a write to not
   * see the result of the seemingly "committed" write.
   */
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

  public int getCommandInteger() {
    try {
      if (command != null) {
        if (command.has(GnsProtocol.COMMAND_INT)) {
          return command.getInt(GnsProtocol.COMMAND_INT);
        }
      }
    } catch (JSONException e) {
      // Just ignore it
    }
    return -1;
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

  // arun: bad hack because of poor legacy code 
  public CommandPacket removeSenderInfo() throws JSONException {
    JSONObject json = this.toJSONObject();
    json.remove(MessageNIOTransport.SNDR_IP_FIELD);
    json.remove(MessageNIOTransport.SNDR_PORT_FIELD);
    return new CommandPacket(json);
  }

  @Override
  public Object getSummary() {
    return new Object() {
      @Override
      public String toString() {
        return getRequestType() + ":"
                + getCommandInteger() + ":"
                + getCommandName() + ":"
                + getServiceName() + ":"
                + getRequestID() + "["
                + getClientAddress() + "]";
      }
    };
  }
}
