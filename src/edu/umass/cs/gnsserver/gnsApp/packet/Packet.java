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

import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.UpdateOperation;
import edu.umass.cs.gnsserver.main.GNS;
import edu.umass.cs.gnsserver.nodeconfig.GNSNodeConfig;
import edu.umass.cs.gnsserver.utils.ResultValue;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.Stringifiable;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.json.JSONArray;

/**
 * So we have these packets see and we convert them back and forth to and from JSON Objects.
 * And send them over UDP and TCP connections. And we have an enum called PacketType that we
 * use to keep track of the type of packet that it is.
 *
 * @author westy
 */
public class Packet {

  /**
   * Defines the type of this packet *
   */
  public final static String PACKET_TYPE = "type";
  //Type of packets

  /**
   * The packet type.
   */
  public enum PacketType implements IntegerPacketType {

    /**
     * DNS
     */
    DNS(1, "edu.umass.cs.gnsserver.gnsApp.packet.DNSPacket"),

    /**
     * ADD_RECORD
     */
    ADD_RECORD(2, "edu.umass.cs.gnsserver.gnsApp.packet.AddRecordPacket"),
    /**
     * ADD_CONFIRM
     */
    ADD_CONFIRM(3, "edu.umass.cs.gnsserver.gnsApp.packet.ConfirmUpdatePacket"),

    /**
     * ADD_BATCH_RECORD - 9/15 NEW
     */
    ADD_BATCH_RECORD(4, "edu.umass.cs.gnsserver.gnsApp.packet.AddBatchRecordPacket"),
    /**
     * COMMAND
     */
    COMMAND(7, "edu.umass.cs.gnsserver.gnsApp.packet.CommandPacket"),
    /**
     * COMMAND_RETURN_VALUE
     */
    COMMAND_RETURN_VALUE(8, "edu.umass.cs.gnsserver.gnsApp.packet.CommandValueReturnPacket"),

    /**
     * REMOVE_RECORD
     */
    REMOVE_RECORD(10, "edu.umass.cs.gnsserver.gnsApp.packet.RemoveRecordPacket"),
    /**
     * REMOVE_CONFIRM
     */
    REMOVE_CONFIRM(11, "edu.umass.cs.gnsserver.gnsApp.packet.ConfirmUpdatePacket"),

    /**
     * UPDATE
     */
    UPDATE(20, "edu.umass.cs.gnsserver.gnsApp.packet.UpdatePacket"), // this is for packets involving the LNS (that is client support -> LNS and LNS -> NS)

    /**
     * UPDATE_CONFIRM
     */
    UPDATE_CONFIRM(21, "edu.umass.cs.gnsserver.gnsApp.packet.ConfirmUpdatePacket"),

    /**
     * REQUEST_ACTIVES
     */
    REQUEST_ACTIVES(30, "edu.umass.cs.gnsserver.gnsApp.packet.RequestActivesPacket"),
    // Admin:

    /**
     * DUMP_REQUEST
     */
    DUMP_REQUEST(40, "edu.umass.cs.gnsserver.gnsApp.packet.admin.DumpRequestPacket"),
    /**
     * SENTINAL
     */
    SENTINAL(41, "edu.umass.cs.gnsserver.gnsApp.packet.admin.SentinalPacket"),
    /**
     * ADMIN_REQUEST
     */
    ADMIN_REQUEST(42, "edu.umass.cs.gnsserver.gnsApp.packet.admin.AdminRequestPacket"),
    /**
     * ADMIN_RESPONSE
     */
    ADMIN_RESPONSE(43, "edu.umass.cs.gnsserver.gnsApp.packet.admin.AdminResponsePacket"),

    /**
     * STATUS
     */
    STATUS(50, "edu.umass.cs.gnsserver.gnsApp.packet.admin.StatusPacket"),
    /**
     * TRAFFIC_STATUS
     */
    TRAFFIC_STATUS(51, "edu.umass.cs.gnsserver.gnsApp.packet.admin.TrafficStatusPacket"),
    /**
     * STATUS_INIT
     */
    STATUS_INIT(52, "edu.umass.cs.gnsserver.gnsApp.packet.admin.StatusInitPacket"),
    // select

    /**
     * SELECT_REQUEST
     */
    SELECT_REQUEST(70, "edu.umass.cs.gnsserver.gnsApp.packet.SelectRequestPacket"),
    /**
     * SELECT_RESPONSE
     */
    SELECT_RESPONSE(71, "edu.umass.cs.gnsserver.gnsApp.packet.SelectResponsePacket"),
    // paxos

    /**
     * PAXOS_PACKET
     */
    PAXOS_PACKET(90, null),
    /**
     * STOP
     */
    STOP(98, null),
    /**
     * NOOP
     */
    NOOP(99, null),
    /**
     * ACTIVE_COORDINATION
     */
    ACTIVE_COORDINATION(120, null), // after transition from old to the new active replicas 
    // is complete, the active replica confirms to replica controller

    /**
     * REPLICA_CONTROLLER_COORDINATION
     */
    REPLICA_CONTROLLER_COORDINATION(121, null), // after transition from old to the 
    //new active replicas is complete, the active replica confirms to replica controller
    // for finite ping pong protocol task example
    /**
     * TEST_PING
     */
    TEST_PING(222, "edu.umass.cs.protocoltask.examples.PingPongPacket"),
    /**
     * TEST_PONG
     */
    TEST_PONG(223, "edu.umass.cs.protocoltask.examples.PingPongPacket"),
    /**
     * TEST_NOOP
     */
    TEST_NOOP(224, null),
    // SPECIAL CASES FOR DNS_SUBTYPE_QUERY PACKETS WHICH USE ONE PACKET FOR ALL THESE
    // these 3 are here for completeness and instrumentation
    /**
     * DNS_SUBTYPE_QUERY
     */
    DNS_SUBTYPE_QUERY(-1, null),
    /**
     * DNS_SUBTYPE_RESPONSE
     */
    DNS_SUBTYPE_RESPONSE(-2, null),
    /**
     * DNS_SUBTYPE_ERROR_RESPONSE
     */
    DNS_SUBTYPE_ERROR_RESPONSE(-3, null);

    private int number;
    private String className;
    private static final Map<Integer, PacketType> map = new HashMap<Integer, PacketType>();

    static {
      for (PacketType type : PacketType.values()) {
        if (map.containsKey(type.getInt())) {
          GNS.getLogger().warning("**** Duplicate ID number for packet type " + type + ": " + type.getInt());
        }
        map.put(type.getInt(), type);
        if (type.className != null) {
          try {
            Class klass = Class.forName(type.className, false, Packet.class.getClassLoader());
            //GNS.getLogger().info(type.name() + "->" + klass.getName());
          } catch (ClassNotFoundException e) {
            GNS.getLogger().warning("Unknown class for " + type.name() + ":" + type.className);
          }
        }
      }
    }

    private PacketType(int number, String className) {
      this.number = number;
      this.className = className;
    }

    @Override
    public int getInt() {
      return number;
    }

    /**
     * Return the class name.
     * 
     * @return the class name
     */
    public String getClassName() {
      return className;
    }

    /**
     * Return the packet type.
     * 
     * @param number
     * @return the packet type
     */
    public static PacketType getPacketType(int number) {
      return map.get(number);
    }
  }

  /**
   * Return the packet sub type.
   * 
   * @param dnsPacket
   * @return the packet sub type
   * @throws JSONException
   */
  public static PacketType getDNSPacketSubType(DNSPacket<String> dnsPacket) throws JSONException {

    if (dnsPacket.isQuery()) { // Query
      return PacketType.DNS_SUBTYPE_QUERY;
    } else if (dnsPacket.isResponse() && !dnsPacket.containsAnyError()) {
      return PacketType.DNS_SUBTYPE_RESPONSE;
    } else if (dnsPacket.containsAnyError()) {
      return PacketType.DNS_SUBTYPE_ERROR_RESPONSE;
    }
    return null;
  }

  // some shorthand helpers
  /**
   * Return the packet type.
   * 
   * @param number
   * @return the packet type
   */
  public static PacketType getPacketType(int number) {
    return PacketType.getPacketType(number);
  }

  /**
   * Return the packet type.
   * 
   * @param json
   * @return the packet type
   * @throws JSONException
   */
  public static PacketType getPacketType(JSONObject json) throws JSONException {
    if (Packet.hasPacketTypeField(json)) {
      return getPacketType(json.getInt(PACKET_TYPE));
    } else {
      throw new JSONException("Packet missing packet type field:" + json.toString());
    }
  }

  /**
   * Returns true if the packet has a type field.
   * 
   * @param json
   * @return true or false
   */
  public static boolean hasPacketTypeField(JSONObject json) {
    return json.has(PACKET_TYPE);
  }

  /**
   * Put the packet type into the packet.
   * 
   * @param json
   * @param type
   * @throws JSONException
   */
  public static void putPacketType(JSONObject json, PacketType type) throws JSONException {
    json.put(PACKET_TYPE, type.getInt());
  }

  private static final String JSON_OBJECT_CLASS = "org.json.JSONObject";
  private static final String STRINGIFIABLE_OBJECT_CLASS = "edu.umass.cs.gnsserver.utils.Stringifiable";

  /**
   * Create an packet instance from a JSON Object that contains a packet plus
   * a Stringifiable instance (same as a packet constructor).
   *
   * @param json
   * @param unstringer
   * @return return the new object
   * @throws org.json.JSONException
   */
  public static Object createInstance(JSONObject json, Stringifiable<String> unstringer)
          throws JSONException {
    try {
      switch (getPacketType(json)) {
        case DNS:
          return new edu.umass.cs.gnsserver.gnsApp.packet.DNSPacket<>(json, unstringer);
        // Add
        case ADD_RECORD:
          return new edu.umass.cs.gnsserver.gnsApp.packet.AddRecordPacket<>(json, unstringer);
        case ADD_CONFIRM:
          return new edu.umass.cs.gnsserver.gnsApp.packet.ConfirmUpdatePacket<>(json, unstringer);
//        case ACTIVE_ADD:
//          return new edu.umass.cs.gnsserver.gnsApp.packet.AddRecordPacket<String>(json, unstringer); // on an add request replica controller sends to active replica
//        case ACTIVE_ADD_CONFIRM:
//          return new edu.umass.cs.gnsserver.gnsApp.packet.AddRecordPacket<String>(json, unstringer); // after adding name, active replica confirms to replica controller
        // new client
        case COMMAND:
          return new edu.umass.cs.gnsserver.gnsApp.packet.CommandPacket(json);
        case COMMAND_RETURN_VALUE:
          return new edu.umass.cs.gnsserver.gnsApp.packet.CommandValueReturnPacket(json);
        // Remove
        case REMOVE_RECORD:
          return new edu.umass.cs.gnsserver.gnsApp.packet.RemoveRecordPacket<>(json, unstringer);
        case REMOVE_CONFIRM:
          return new edu.umass.cs.gnsserver.gnsApp.packet.ConfirmUpdatePacket<>(json, unstringer);
//        case ACTIVE_REMOVE:
//          return null; // on a remove request, replica controller sends to active replica
//       case RC_REMOVE:
//          return new edu.umass.cs.gnsserver.gnsApp.packet.RemoveRecordPacket<String>(json, unstringer);
        // Update
        case UPDATE:
          return new edu.umass.cs.gnsserver.gnsApp.packet.UpdatePacket<>(json, unstringer); // this is for packets involving the LNS (that is client support -> LNS and LNS -> NS)
        case UPDATE_CONFIRM:
          return new edu.umass.cs.gnsserver.gnsApp.packet.ConfirmUpdatePacket<>(json, unstringer);
        // Lookup actives
        case REQUEST_ACTIVES:
          return new edu.umass.cs.gnsserver.gnsApp.packet.RequestActivesPacket<>(json, unstringer);
        // Admin:
        case DUMP_REQUEST:
          return new edu.umass.cs.gnsserver.gnsApp.packet.admin.DumpRequestPacket<>(json, unstringer);
        case SENTINAL:
          return new edu.umass.cs.gnsserver.gnsApp.packet.admin.SentinalPacket(json);
        case ADMIN_REQUEST:
          return new edu.umass.cs.gnsserver.gnsApp.packet.admin.AdminRequestPacket(json);
        case ADMIN_RESPONSE:
          return new edu.umass.cs.gnsserver.gnsApp.packet.admin.AdminResponsePacket(json);
        // status
        case STATUS:
          return new edu.umass.cs.gnsserver.gnsApp.packet.admin.StatusPacket(json);
        case TRAFFIC_STATUS:
          return new edu.umass.cs.gnsserver.gnsApp.packet.admin.TrafficStatusPacket(json);
        case STATUS_INIT:
          return new edu.umass.cs.gnsserver.gnsApp.packet.admin.StatusInitPacket(json);
        // select
        case SELECT_REQUEST:
          return new edu.umass.cs.gnsserver.gnsApp.packet.SelectRequestPacket<>(json, unstringer);
        case SELECT_RESPONSE:
          return new edu.umass.cs.gnsserver.gnsApp.packet.SelectResponsePacket<>(json, unstringer);
        // paxos
        case PAXOS_PACKET:
          return null;
        case STOP:
          return new edu.umass.cs.gnsserver.gnsApp.packet.StopPacket(json);
        case NOOP:
          return new edu.umass.cs.gnsserver.gnsApp.packet.NoopPacket(json);
        // coordination
        case ACTIVE_COORDINATION:
          return null; // after transition from old to the new active replicas is complete, the active replica confirms to replica controller
        case REPLICA_CONTROLLER_COORDINATION:
          return null; // after transition from old to the new active replicas is complete, the active replica confirms to replica controller
        // for finite ping pong protocol task example
        case TEST_PING:
          return new edu.umass.cs.protocoltask.examples.PingPongPacket(json);
        case TEST_PONG:
          return new edu.umass.cs.protocoltask.examples.PingPongPacket(json);
        case TEST_NOOP:
          return null;
        // SPECIAL CASES FOR DNS_SUBTYPE_QUERY PACKETS WHICH USE ONE PACKET FOR ALL THESE
        // these 3 are here for completeness and instrumentation
        case DNS_SUBTYPE_QUERY:
          return null;
        case DNS_SUBTYPE_RESPONSE:
          return null;
        case DNS_SUBTYPE_ERROR_RESPONSE:
          return null;
        default:
          GNS.getLogger().severe("Packet type not found: " + getPacketType(json) + " JSON: " + json);
          return null;
      }
    } catch (ParseException e) {
      throw new JSONException(e);
    }
  }

  ///
  /// PACKET SENDING CODE THAT WE KEEP AROUND SO THAT THE ADMIN SIDE OF THINGS
  /// IS SEPARATE FROM THE NIO SIDE.
  ///
  /**
   * Delimiter that separates size from data in each frame transmitted *
   */
  public static final String HEADER_PATTERN = "&";

  /**
   * **
   * Reads bytes until we see delimiter ":". All bytes before ":" indicates the size of the data. Bytes after ":" is the actual
   * data.
   *
   * @param inStream
   * @return Size of a frame (packet) or -1 if the input stream is closed *
   */
  public static int getDataSize(InputStream inStream) {

    String input;
    String vectorSizeStr = "";
    byte[] tempBuffer = new byte[1];

    try {
      //NOTE THAT HEADER_PATTERN size could be greater than 1 in which case this code doesn't work
      // find first instance of HEADER_PATTERN (should be first char)
      do {
        if (inStream.read(tempBuffer, 0, 1) == -1) {
          return -1;
        }
        input = new String(tempBuffer);
      } while (input.compareTo(HEADER_PATTERN) != 0);

      //Keep reading from input stream until we see HEADER_PATTERN again. The bytes 
      //before second HEADER_PATTERN represents the size of the frame. The bytes after
      //second HEADER_PATTERN is the actual frame.
      do {
        if (inStream.read(tempBuffer, 0, 1) == -1) {
          return -1;
        }

        input = new String(tempBuffer);
        if (input.compareTo(HEADER_PATTERN) != 0) {
          vectorSizeStr += input;
        }
      } while (input.compareTo(HEADER_PATTERN) != 0);

    } catch (IOException e) {
      return -1;
    }

    return Integer.parseInt(vectorSizeStr.trim());
  }

  /**
   * **
   * Reads bytes from the input stream until we have read bytes equal the size of a frame (packet) and returns a JSONObject that
   * represents the frame.
   *
   * @param input Input stream to read the frame.
   * @param sizeOfFrame Size of a frame (packet)
   * @throws java.io.IOException
   * @throws org.json.JSONException *
   */
  private static JSONObject getJSONObjectFrame(InputStream input, int sizeOfFrame)
          throws IOException, JSONException {
    byte[] jsonByte = new byte[sizeOfFrame];
    int tempSize = input.read(jsonByte);

    //Keep reading bytes until we have read number of bytes equal the expected
    //size of the frame.
    while (tempSize != sizeOfFrame) {
      tempSize += input.read(jsonByte, tempSize, sizeOfFrame - tempSize);
    }

    JSONObject json = new JSONObject(new String(jsonByte));
    return json;
  }

  /**
   * **
   * Reads bytes from the input stream until we have read bytes equal the size of a frame (packet) and returns a JSONObject that
   * represents the frame.
   *
   * @param socket Socket on which the frame has arrived
   * @return Returns JSONObject representing the packet. Returns <i>null</i> if the socket is closed.
   * @throws java.io.IOException
   * @throws org.json.JSONException *
   */
  public static JSONObject getJSONObjectFrame(Socket socket)
          throws IOException, JSONException {
    if (socket == null) {
      throw new IOException("Socket is null");
    }

    InputStream input = socket.getInputStream();
    //Get the size of the packet
    int sizeOfPacket = Packet.getDataSize(input);

    //Check for error: input stream reached the end of file
    if (sizeOfPacket == -1) {
      throw new IOException("Input stream reached the end of file");
    }

    //Read the packet from the input stream
    return Packet.getJSONObjectFrame(input, sizeOfPacket);
  }

  /**
   * Sends a packet to a name server using TCP.
   *
   * @param gnsNodeConfig
   * @param json JsonObject representing the packet
   * @param nameserverId Name server id
   * @param portType Type of port
   * @return Returns the Socket over which the packet was sent, or null if the port type is incorrect.
   * @throws java.io.IOException *
   */
  @SuppressWarnings("unchecked")
  public static Socket sendTCPPacket(GNSNodeConfig gnsNodeConfig, JSONObject json, Object nameserverId, GNS.PortType portType) throws IOException {
    int port = gnsNodeConfig.getPortForTopLevelNode(nameserverId, portType);
    if (port == -1) {
      GNS.getLogger().warning("sendTCPPacket:: FAIL, BAD PORT! to: " + nameserverId + " json: " + json.toString());
      throw new IOException("Invalid port number " + port);
    }

    InetAddress addr = gnsNodeConfig.getNodeAddress(nameserverId);
    if (addr == null) {
      GNS.getLogger().warning("sendTCPPacket:: FAIL, BAD ADDRESS! to: " + nameserverId + " port: " + port + " json: " + json.toString());
      return null;
    }
    return sendTCPPacket(json, new InetSocketAddress(addr, port));
  }

  /**
   * Send a TCP packet.
   * 
   * @param json
   * @param addr
   * @return a Socket
   * @throws IOException
   */
  public static Socket sendTCPPacket(JSONObject json, InetSocketAddress addr) throws IOException {
    GNS.getLogger().finer("sendTCPPacket:: to " + addr.getHostString() + ":" + addr.getPort() + " json: " + json.toString());
    Socket socket = new Socket(addr.getHostString(), addr.getPort());
    sendTCPPacket(json, socket);
    return socket;
  }

  /**
   * Sends a packet to a name server using TCP.
   *
   * @param json JsonObject representing the packet //
   * @param socket Socket on which to send the packet
   * @throws java.io.IOException *
   */
  public static void sendTCPPacket(JSONObject json, Socket socket) throws IOException {
    if (json == null || socket == null) {
      return;
    }
    String packet = json.toString();
    Integer jsonSize = packet.getBytes().length;
    String msg = Packet.HEADER_PATTERN + jsonSize.toString() + Packet.HEADER_PATTERN + packet;

    GNS.getLogger().finer("sendTCPPacket:: to: " + socket.getInetAddress().getHostName() + ":" + socket.getPort() + " json: " + json.toString());
    PrintWriter output = new PrintWriter(socket.getOutputStream(), true);
    output.println(msg);
    output.flush();
  }

  /**
   * Multicast TCP packet to all name servers in <i>nameServerIds</i>. This method excludes name server id in
   * <i>excludeNameServers</i>
   *
   * @param gnsNodeConfig
   * @param nameServerIds Set of name server ids where packet is sent
   * @param json JSONObject representing the packet
   * @param numRetry Number of re-try if the connection fails before successfully sending the packet.
   * @param portType Type of port to connect
   * @param excludeNameServers *
   */
  public static void multicastTCP(GNSNodeConfig gnsNodeConfig, Set nameServerIds, JSONObject json, int numRetry,
          GNS.PortType portType, Set excludeNameServers) {
    int tries;
    for (Object id : nameServerIds) {
      if (excludeNameServers != null && excludeNameServers.contains(id)) {
        continue;
      }

      tries = 0;
      do {
        tries += 1;
        try {
          Socket socket = Packet.sendTCPPacket(gnsNodeConfig, json, (String) id, portType);
          if (socket != null) {
            socket.close();
          }
          break;
        } catch (IOException e) {
          GNS.getLogger().severe("Exception: socket closed by nameserver " + id);
          e.printStackTrace();
        }
      } while (tries < numRetry);
    }
  }

  //
  // DEBUGGING AIDS: Could move them somewhere else
  //
  /**
   * A debugging aid that returns true if the packet is not a Paxos packet or another "chatty" packet.
   *
   * @param jsonData
   * @return returns true if the packet is not chatty
   */
  public static boolean filterOutChattyPackets(JSONObject jsonData) {
    try {
//      if (PaxosPacket.hasPacketTypeField(jsonData)) {
//        // handle Paxos packets
//        PaxosPacketType packetType = PaxosPacket.getPacketType(jsonData);
//        if (packetType != PaxosPacketType.FAILURE_DETECT
//                && packetType != PaxosPacketType.FAILURE_RESPONSE) {
//          return true;
//        }
//      } else {
      // handle Regular packets
      PacketType packetType = getPacketType(jsonData);
      if (//packetType != PacketType.NAME_SERVER_LOAD && 
              packetType != PacketType.ACTIVE_COORDINATION
              && packetType != PacketType.REPLICA_CONTROLLER_COORDINATION) {
        return true;
      }
      //}
    } catch (JSONException e) {

    }
    return false;
  }

  /**
   * A debugging aid that returns a string identifying the packet type or "Unknown" if it cannot be determined.
   *
   * @param json
   * @return a string
   */
  public static String getPacketTypeStringSafe(JSONObject json) {
    try {
//      if (PaxosPacket.hasPacketTypeField(json)) {
//        // handle Paxos packets
//        return PaxosPacket.getPacketType(json).toString();
//      } else {
      // handle Regular packets
      return getPacketType(json).toString();
      //}
    } catch (JSONException e) {
      return "Unknown";
    }
  }

  /**
   * Test
   *
   * @param args
   * @throws java.io.IOException *
   */
  public static void main(String[] args) throws IOException {
    Stringifiable<String> unstringer = new Stringifiable<String>() {
      @Override
      public String valueOf(String nodeAsString) {
        return nodeAsString;
      }

      @Override
      public Set<String> getValuesFromStringSet(Set strNodes) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
      }

      @Override
      public Set<String> getValuesFromJSONArray(JSONArray array) throws JSONException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
      }
    };
    System.out.println(PacketType.DNS);
    System.out.println(getPacketType(5));
    System.out.println(getPacketType(5).toString());
    System.out.println(PacketType.valueOf("REQUEST_ACTIVES").toString());
    System.out.println(PacketType.valueOf("REQUEST_ACTIVES").getInt());
    // createInstance testing
    ResultValue x = new ResultValue();
    x.add("12345678");
    UpdatePacket<String> up = new UpdatePacket<String>(null, 32234234, 123, "12322323",
            "EdgeRecord", x, null, -1, null, UpdateOperation.SINGLE_FIELD_APPEND_WITH_DUPLICATION, null, "123",
            GNS.DEFAULT_TTL_SECONDS, null, null, null);
    System.out.println(up.toString());
    JSONObject json = null;
    try {
      json = up.toJSONObject();
    } catch (JSONException e) {
      System.out.println("Problem converting packet to JSON: " + e);
    }
    if (json != null) {
      Object object = null;
      try {
        object = createInstance(json, unstringer);
      } catch (JSONException e) {
        System.out.println("Problem creating instance: " + e);
      }
      if (object != null) {
        System.out.println(object.toString());
      } else {
        System.out.println("OBJECT IS NULL");
      }
    }
  }
}
