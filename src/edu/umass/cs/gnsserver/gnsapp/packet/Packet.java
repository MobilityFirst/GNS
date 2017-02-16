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
 *  Initial developer(s): Westy
 *
 */
package edu.umass.cs.gnsserver.gnsapp.packet;

import edu.umass.cs.gnscommon.packets.AdminCommandPacket;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.packets.ResponsePacket;
import edu.umass.cs.gnsserver.gnsapp.packet.admin.AdminRequestPacket;
import edu.umass.cs.gnsserver.gnsapp.packet.admin.AdminResponsePacket;
import edu.umass.cs.gnsserver.gnsapp.packet.admin.DumpRequestPacket;
import edu.umass.cs.gnsserver.gnsapp.packet.admin.SentinalPacket;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.gnsserver.nodeconfig.PortOffsets;
import edu.umass.cs.gnsserver.nodeconfig.GNSNodeConfig;
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
import java.util.logging.Level;

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
     * COMMAND
     */
    COMMAND(7, CommandPacket.class.getCanonicalName()),
    /**
     * COMMAND_RETURN_VALUE
     */
    COMMAND_RETURN_VALUE(8, ResponsePacket.class.getCanonicalName()),
    
    /**
     * A variant of {@link CommandPacket} that is meant to be used for
     * internal requests spawned by chains in {@link CommandType}.
     */
    INTERNAL_COMMAND(9, InternalCommandPacket.class.getCanonicalName()),
    
    /**
     * ADMIN_COMMAND
     * A variant of {@link CommandPacket} that is used for client requests that require mutual authentication.
     */
    ADMIN_COMMAND(10, AdminCommandPacket.class.getCanonicalName()),

    
    /**
     * DUMP_REQUEST
     */
    DUMP_REQUEST(40, DumpRequestPacket.class.getCanonicalName()),
    /**
     * SENTINAL
     */
    SENTINAL(41, SentinalPacket.class.getCanonicalName()),
    /**
     * ADMIN_REQUEST
     */
    ADMIN_REQUEST(42, AdminRequestPacket.class.getCanonicalName()),
    /**
     * ADMIN_RESPONSE
     */
    ADMIN_RESPONSE(43, AdminResponsePacket.class.getCanonicalName()),
    /**
     * SELECT_REQUEST
     */
    SELECT_REQUEST(70, SelectRequestPacket.class.getCanonicalName()),
    /**
     * SELECT_RESPONSE
     */
    SELECT_RESPONSE(71, SelectResponsePacket.class.getCanonicalName()),
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
     * TEST_NOOP
     */
    TEST_NOOP(224, null),;
    private final int number;
    private String className;
    private static final Map<Integer, PacketType> map = new HashMap<>();

    static {
      for (PacketType type : PacketType.values()) {
        if (map.containsKey(type.getInt())) {
          GNSConfig.getLogger().log(Level.WARNING,
                  "**** Duplicate ID number for packet type {0}: {1}", new Object[]{type, type.getInt()});
        }
        map.put(type.getInt(), type);
        if (type.className != null) {
          try {
            Class<?> klass = Class.forName(type.className, false, Packet.class.getClassLoader());
            GNSConfig.getLogger().log(Level.FINER, "{0}->{1}", 
                    new Object[]{type.name(), klass.getName()});
          } catch (ClassNotFoundException e) {
            GNSConfig.getLogger().log(Level.WARNING,
                    "Unknown class for {0}:{1}", new Object[]{type.name(), type.className});
          }
        }
      }
    }

    private PacketType(int number, String className) {
      this.number = number;
      this.className = className;
    }

    /**
     *
     * @return the int
     */
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
      PacketType t = map.get(number);
//      assert(t!=null) : number;
      if(t==null) throw new RuntimeException("Unrecognized packet type " + number);
      return t;
    }
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

    PacketType packetType = null;
    try {
      packetType = getPacketType(json);
    } catch (JSONException e) {
      int typeInt = json.optInt(PACKET_TYPE, -1);
      GNSConfig.getLogger().log(Level.INFO, "Problem getting packet type {0} in {1}",
              new Object[]{typeInt, json});
    }
    if (packetType != null) {
      switch (packetType) {
        // Client
      case ADMIN_COMMAND:
    	  return new edu.umass.cs.gnscommon.packets.AdminCommandPacket(json);
        case COMMAND:
          return new edu.umass.cs.gnscommon.packets.CommandPacket(json);
        case COMMAND_RETURN_VALUE:
          return new edu.umass.cs.gnscommon.packets.ResponsePacket(json);
        case INTERNAL_COMMAND:
        	return new edu.umass.cs.gnsserver.gnsapp.packet.InternalCommandPacket(json);
        	
        // Admin:
        case DUMP_REQUEST:
          return new edu.umass.cs.gnsserver.gnsapp.packet.admin.DumpRequestPacket<>(json, unstringer);
        case SENTINAL:
          return new edu.umass.cs.gnsserver.gnsapp.packet.admin.SentinalPacket(json);
        case ADMIN_REQUEST:
          return new edu.umass.cs.gnsserver.gnsapp.packet.admin.AdminRequestPacket(json);
        case ADMIN_RESPONSE:
          try {
            return new edu.umass.cs.gnsserver.gnsapp.packet.admin.AdminResponsePacket(json);
          } catch (ParseException e) {
            throw new JSONException(e);
          }
        // select
        case SELECT_REQUEST:
          return new edu.umass.cs.gnsserver.gnsapp.packet.SelectRequestPacket(json);
        case SELECT_RESPONSE:
          return new edu.umass.cs.gnsserver.gnsapp.packet.SelectResponsePacket(json);
        // paxos
        case PAXOS_PACKET:
          return null;
        case STOP:
          return new edu.umass.cs.gnsserver.gnsapp.packet.StopPacket(json);
        case TEST_NOOP:
          return null;
        default:
          GNSConfig.getLogger().log(Level.SEVERE,
                  "Packet type not found: {0} JSON: {1}", new Object[]{getPacketType(json), json});
      }
    }
    return null;
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
   * Reads bytes until we see delimiter HEADER_PATTERN. All bytes before HEADER_PATTERN indicates the size of the data. Bytes after ":" is the actual
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
  @SuppressWarnings("javadoc")
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
  public static Socket sendTCPPacket(GNSNodeConfig<String> gnsNodeConfig, JSONObject json,
          String nameserverId, PortOffsets portType) throws IOException {
    int port = gnsNodeConfig.getPortForTopLevelNode(nameserverId, portType);
    if (port == -1) {
      GNSConfig.getLogger().log(Level.WARNING,
              "sendTCPPacket:: FAIL, BAD PORT! to: {0} json: {1}", new Object[]{nameserverId, json.toString()});
      throw new IOException("Invalid port number " + port);
    }

    InetAddress addr = gnsNodeConfig.getNodeAddress(nameserverId);
    if (addr == null) {
      GNSConfig.getLogger().log(Level.WARNING,
              "sendTCPPacket:: FAIL, BAD ADDRESS! to: {0} port: {1} json: {2}",
              new Object[]{nameserverId, port, json.toString()});
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
    GNSConfig.getLogger().log(Level.FINER,
            "sendTCPPacket:: to {0}:{1} json: {2}",
            new Object[]{addr.getHostString(), addr.getPort(), json.toString()});
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

    GNSConfig.getLogger().log(Level.FINER,
            "sendTCPPacket:: to: {0}:{1} json: {2}",
            new Object[]{socket.getInetAddress().getHostName(), socket.getPort(), json.toString()});
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
  public static void multicastTCP(GNSNodeConfig<String> gnsNodeConfig,
          Set<String> nameServerIds, JSONObject json, int numRetry,
          PortOffsets portType, Set<Object> excludeNameServers) {
    int tries;
    for (String id : nameServerIds) {
      if (excludeNameServers != null && excludeNameServers.contains(id)) {
        continue;
      }

      tries = 0;
      do {
        tries += 1;
        try {
          Socket socket = Packet.sendTCPPacket(gnsNodeConfig, json, id, portType);
          if (socket != null) {
            socket.close();
          }
          break;
        } catch (IOException e) {
          GNSConfig.getLogger().log(Level.SEVERE, "Exception: socket closed by nameserver {0}", id);
          e.printStackTrace();
        }
      } while (tries < numRetry);
    }
  }

  /**
   * A debugging aid that returns a string identifying the packet type or "Unknown" if it cannot be determined.
   *
   * @param json
   * @return a string
   */
  public static String getPacketTypeStringSafe(JSONObject json) {
    try {
      return getPacketType(json).toString();
      //}
    } catch (JSONException e) {
      return "Unknown";
    }
  }
}
