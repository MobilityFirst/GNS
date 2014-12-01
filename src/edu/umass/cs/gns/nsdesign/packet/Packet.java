/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.nsdesign.packet;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nio.IntegerPacketType;
import edu.umass.cs.gns.nsdesign.nodeconfig.GNSNodeConfig;
import edu.umass.cs.gns.paxos.paxospacket.PaxosPacket;
import edu.umass.cs.gns.paxos.paxospacket.PaxosPacketType;
import org.json.JSONException;
import org.json.JSONObject;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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

  public enum PacketType implements IntegerPacketType {

    DNS(1),
    // Add
    ADD_RECORD(2),
    CONFIRM_ADD(3),
    ACTIVE_ADD(4), // on an add request replica controller sends to active replica
    ACTIVE_ADD_CONFIRM(5), // after adding name, active replica confirms to replica controller

    // new client
    COMMAND(7),
    COMMAND_RETURN_VALUE(8),
    // Remove
    REMOVE_RECORD(10),
    CONFIRM_REMOVE(11),
    ACTIVE_REMOVE(12), // on a remove request, replica controller sends to active replica
    ACTIVE_REMOVE_CONFIRM(13), // after removing name, active replica confirms to replica controller
    RC_REMOVE(14),
    // Update
    UPDATE(20), // this is for packets involving the LNS (that is client support -> LNS and LNS -> NS)
    CONFIRM_UPDATE(21),
    // Lookup actives
    REQUEST_ACTIVES(30),
    LNS_TO_NS_COMMAND(35),
    // Admin:
    DUMP_REQUEST(40),
    SENTINAL(41),
    ADMIN_REQUEST(42),
    ADMIN_RESPONSE(43),
    // status
    STATUS(50),
    TRAFFIC_STATUS(51),
    STATUS_INIT(52),
    // select
    SELECT_REQUEST(70),
    SELECT_RESPONSE(71),
    // stats collection for names
    NAMESERVER_SELECTION(80),
    NAME_RECORD_STATS_REQUEST(81),
    NAME_RECORD_STATS_RESPONSE(82),
    ACTIVE_NAMESERVER_INFO(83),
    // paxos
    PAXOS_PACKET(90),
    // group change
    NEW_ACTIVE_PROPOSE(100),
    OLD_ACTIVE_STOP(101),
    OLD_ACTIVE_STOP_CONFIRM_TO_PRIMARY(102),
    DELETE_OLD_ACTIVE_STATE(103), //
    NEW_ACTIVE_START(104),
    NEW_ACTIVE_START_FORWARD(105),
    NEW_ACTIVE_START_RESPONSE(106),
    NEW_ACTIVE_START_CONFIRM_TO_PRIMARY(107),
    NEW_ACTIVE_START_PREV_VALUE_REQUEST(108),
    NEW_ACTIVE_START_PREV_VALUE_RESPONSE(109),
    GROUP_CHANGE_COMPLETE(110),
    // coordination
    ACTIVE_COORDINATION(120), // after transition from old to the new active replicas is complete, the active replica confirms to replica controller
    REPLICA_CONTROLLER_COORDINATION(121), // after transition from old to the new active replicas is complete, the active replica confirms to replica controller

    // deprecated packet types:
    UPDATE_NAMESERVER(201),
    ACTIVE_NAMESERVER_UPDATE(202),
    NAME_SERVER_LOAD(203),
    
    // for finite ping pong protocol task example
    TEST_PING(222),
    TEST_PONG(223),
    
    TEST_NOOP(224),
    
    // SPECIAL CASES FOR DNS_SUBTYPE_QUERY PACKETS WHICH USE ONE PACKET FOR ALL THESE
    // these 3 are here for completeness and instrumentation
    DNS_SUBTYPE_QUERY(-1),
    DNS_SUBTYPE_RESPONSE(-2),
    DNS_SUBTYPE_ERROR_RESPONSE(-3);

    private int number;
    private static final Map<Integer, PacketType> map = new HashMap<Integer, PacketType>();

    static {
      for (PacketType type : PacketType.values()) {
        if (map.containsKey(type.getInt())) {
          GNS.getLogger().warning("**** Duplicate ID number for packet type " + type + ": " + type.getInt());
        }
        map.put(type.getInt(), type);
      }
    }

    private PacketType(int number) {
      this.number = number;
    }

    @Override
    public int getInt() {
      return number;
    }

    public static PacketType getPacketType(int number) {
      return map.get(number);
    }
  }

  public static PacketType getDNSPacketSubType(DNSPacket dnsPacket) throws JSONException {

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
  public static PacketType getPacketType(int number) {
    return PacketType.getPacketType(number);
  }

  public static PacketType getPacketType(JSONObject json) throws JSONException {
    if (Packet.hasPacketTypeField(json)) {
      return getPacketType(json.getInt(PACKET_TYPE));
    } else {
      throw new JSONException("Packet missing packet type field:" + json.toString());
    }
  }

  public static boolean hasPacketTypeField(JSONObject json) {
    return json.has(PACKET_TYPE);
  }

  public static void putPacketType(JSONObject json, PacketType type) throws JSONException {
    json.put(PACKET_TYPE, type.getInt());
  }
  /**
   * Delimiter that separates size from data in each frame transmitted *
   */
  //public final static String DELIMITER = ":";

  public static final String HEADER_PATTERN = "&";

  /**
   * **
   * Reads bytes until we see delimiter ":". All bytes before ":" indicates the size of the data. Bytes after ":" is the actual
   * data.
   *
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
   * **
   * Send a response to a name server using UDP
   *
   * //
   *
   * @param socket DatagramSocket over which the packet is sent
   * @param json JsonObject representing the packet
   * @throws java.io.IOException *
   */
  public static void sendUDPPacket(DatagramSocket socket, JSONObject json, InetAddress address, int port) throws IOException {
    if (address == null || json == null || socket == null || port == -1) {
      GNS.getLogger().warning("sendUDPPacket:: FAIL! address: " + address + " port: " + port + " json: " + json.toString());
      return;
    }

//    GNRS.getLogger().finer("sendUDPPacket:: address: " + address.getHostName() + " port: " + port + " json: " + json.toString());
    byte[] buffer = json.toString().getBytes();
    DatagramPacket packet = new DatagramPacket(buffer, buffer.length, address, port);
    socket.send(packet);
  }

  /**
   * Sends a packet to a name server using TCP.
   *
   * @param json JsonObject representing the packet
   * @param nameserverId Name server id
   * @param portType Type of port
   * @return Returns the Socket over which the packet was sent, or null if the port type is incorrect.
   * @throws java.io.IOException *
   */
  public static Socket sendTCPPacket(GNSNodeConfig gnsNodeConfig, JSONObject json, Object nameserverId, GNS.PortType portType) throws IOException {
    int port = gnsNodeConfig.getPort(nameserverId, portType);
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
   * Multicast TCP packet to all name servers in <i>nameServerIds</i>.
   *
   * @param nameServerIds Set of name server ids where packet is sent
   * @param json JSONObject representing the packet
   * @param numRetry Number of re-try if the connection fails before successfully sending the packet.
   * @param portType Type of port to connect *
   */
  public static void multicastTCP(GNSNodeConfig gnsNodeConfig, Set nameServerIds, JSONObject json, int numRetry, GNS.PortType portType) {
    multicastTCP(gnsNodeConfig, nameServerIds, json, numRetry, portType, -1);
  }

  /**
   * Multicast TCP packet to all name servers in <i>nameServerIds</i>. Excludes name server whose id match
   * <i>excludeNameServerId</i>
   *
   * @param nameServerIds Set of name server ids where packet is sent
   * @param json JSONObject representing the packet
   * @param numRetry Number of re-try if the connection fails before successfully sending the packet.
   * @param portType Type of port to connect
   * @param excludeNameServerId Id of name server which is excluded from multicast *
   */
  public static void multicastTCP(GNSNodeConfig gnsNodeConfig, Set nameServerIds, JSONObject json, int numRetry, 
          GNS.PortType portType, int excludeNameServerId) {
    multicastTCP(gnsNodeConfig, nameServerIds, json, numRetry, portType, new HashSet<Integer>(Arrays.asList(excludeNameServerId)));
  }

  /**
   * Multicast TCP packet to all name servers in <i>nameServerIds</i>. This method excludes name server id in
   * <i>excludeNameServers</i>
   *
   * @param nameServerIds Set of name server ids where packet is sent
   * @param json JSONObject representing the packet
   * @param numRetry Number of re-try if the connection fails before successfully sending the packet.
   * @param portType Type of port to connect
   * @param excludeNameServers *
   */
  public static void multicastTCP(GNSNodeConfig gnsNodeConfig, Set nameServerIds, JSONObject json, int numRetry, 
          GNS.PortType portType, Set<Integer> excludeNameServers) {
    int tries;
    for (Object id : nameServerIds) {
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
          GNS.getLogger().severe("Exception: socket closed by nameserver " + id);
          e.printStackTrace();
        }
      } while (tries < numRetry);
    }
  }

  /**
   * Sends a packet to the closest name server in <i>nameServerIds</i> over TCP. If the connection fails, the packet is sent to the
   * next closest name server untill <i>numRetry</i> attemps have been made to successfully send the packet.
   *
   * @param nameServerIds Set of name server ids
   * @param json JSONObject representing the packet
   * @param numRetry Number of attempts if the connection fails before successfully sending the packet.
   * @param portType Type of port to connect
   * @return Returns the <i>Socket</i> over which the packet was transmitted. Returns <i>null</i> if no connection was successful in
   * sending the packet. *
   */
  public static Socket sendTCPPacketToClosestNameServer(GNSNodeConfig gnsNodeConfig, Set nameServerIds, JSONObject json,
          int numRetry, GNS.PortType portType) {

    int attempt = 0;
    Set<Object> excludeNameServer = new HashSet<>();
    Object id;

    do {
      attempt++;
      id = (Object) gnsNodeConfig.getClosestServer(nameServerIds, excludeNameServer);
      excludeNameServer.add(id);
      try {
        return Packet.sendTCPPacket(gnsNodeConfig, json, id, portType);
      } catch (IOException e) {
        GNS.getLogger().severe("Exception: socket closed by nameserver " + id);
        e.printStackTrace();
      }
    } while (attempt < numRetry);
    return null;
  }

  //
  // DEBUGGING AIDS: Could move them somewhere else
  //
  /**
   * A debugging aid that returns true if the packet is not a Paxos packet or another "chatty" packet.
   *
   * @param jsonData
   * @return
   */
  public static boolean filterOutChattyPackets(JSONObject jsonData) {
    try {
      if (PaxosPacket.hasPacketTypeField(jsonData)) {
        // handle Paxos packets
        PaxosPacketType packetType = PaxosPacket.getPacketType(jsonData);
        if (packetType != PaxosPacketType.FAILURE_DETECT
                && packetType != PaxosPacketType.FAILURE_RESPONSE) {
          return true;
        }
      } else {
        // handle Regular packets
        PacketType packetType = getPacketType(jsonData);
        if (packetType != PacketType.NAME_SERVER_LOAD
                && packetType != PacketType.ACTIVE_COORDINATION
                && packetType != PacketType.REPLICA_CONTROLLER_COORDINATION) {
          return true;
        }
      }
    } catch (JSONException e) {

    }
    return false;
  }

  /**
   * A debugging aid that returns a string identifying the packet type or "Unknown" if it cannot be determined.
   *
   * @param json
   * @return
   */
  public static String getPacketTypeStringSafe(JSONObject json) {
    try {
      if (PaxosPacket.hasPacketTypeField(json)) {
        // handle Paxos packets
        return PaxosPacket.getPacketType(json).toString();
      } else {
        // handle Regular packets
        return getPacketType(json).toString();
      }
    } catch (JSONException e) {
      return "Unknown";
    }
  }

  /**
   * Test
   *
   * @throws java.io.IOException *
   */
  public static void main(String[] args) throws IOException {
    System.out.println(PacketType.DNS);
    System.out.println(getPacketType(5));
    System.out.println(getPacketType(5).toString());
    System.out.println(PacketType.valueOf("REQUEST_ACTIVES").toString());
    System.out.println(PacketType.valueOf("REQUEST_ACTIVES").getInt());
  }
}
