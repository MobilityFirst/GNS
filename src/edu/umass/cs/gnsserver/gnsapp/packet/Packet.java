
package edu.umass.cs.gnsserver.gnsapp.packet;

import edu.umass.cs.gnscommon.packets.AdminCommandPacket;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnscommon.packets.ResponsePacket;
import edu.umass.cs.gnsserver.gnsapp.packet.admin.AdminRequestPacket;
import edu.umass.cs.gnsserver.gnsapp.packet.admin.AdminResponsePacket;
import edu.umass.cs.gnsserver.gnsapp.packet.admin.DumpRequestPacket;
import edu.umass.cs.gnsserver.gnsapp.packet.admin.SentinalPacket;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.gnsserver.main.OldHackyConstants;
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


public class Packet {


  public final static String PACKET_TYPE = "type";
  //Type of packets


  public enum PacketType implements IntegerPacketType {

    COMMAND(7, CommandPacket.class.getCanonicalName()),

    COMMAND_RETURN_VALUE(8, ResponsePacket.class.getCanonicalName()),
    

    INTERNAL_COMMAND(9, InternalCommandPacket.class.getCanonicalName()),
    

    ADMIN_COMMAND(10, AdminCommandPacket.class.getCanonicalName()),

    

    DUMP_REQUEST(40, DumpRequestPacket.class.getCanonicalName()),

    SENTINAL(41, SentinalPacket.class.getCanonicalName()),

    ADMIN_REQUEST(42, AdminRequestPacket.class.getCanonicalName()),

    ADMIN_RESPONSE(43, AdminResponsePacket.class.getCanonicalName()),

    SELECT_REQUEST(70, SelectRequestPacket.class.getCanonicalName()),

    SELECT_RESPONSE(71, SelectResponsePacket.class.getCanonicalName()),
    // paxos


    PAXOS_PACKET(90, null),

    STOP(98, null),

    NOOP(99, null),

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
            //GNS.getLogger().info(type.name() + "->" + klass.getName());
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


    @Override
    public int getInt() {
      return number;
    }


    public String getClassName() {
      return className;
    }


    public static PacketType getPacketType(int number) {
      PacketType t = map.get(number);
//      assert(t!=null) : number;
      if(t==null) throw new RuntimeException("Unrecognized packet type " + number);
      return t;
    }
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

  private static final String JSON_OBJECT_CLASS = "org.json.JSONObject";
  private static final String STRINGIFIABLE_OBJECT_CLASS = "edu.umass.cs.gnsserver.utils.Stringifiable";


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
          return new edu.umass.cs.gnsserver.gnsapp.packet.SelectRequestPacket<>(json, unstringer);
        case SELECT_RESPONSE:
          return new edu.umass.cs.gnsserver.gnsapp.packet.SelectResponsePacket<>(json, unstringer);
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

  public static final String HEADER_PATTERN = "&";


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


  @SuppressWarnings("unchecked")
  public static Socket sendTCPPacket(GNSNodeConfig<String> gnsNodeConfig, JSONObject json,
          String nameserverId, OldHackyConstants.PortType portType) throws IOException {
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


  public static Socket sendTCPPacket(JSONObject json, InetSocketAddress addr) throws IOException {
    GNSConfig.getLogger().log(Level.FINER,
            "sendTCPPacket:: to {0}:{1} json: {2}",
            new Object[]{addr.getHostString(), addr.getPort(), json.toString()});
    Socket socket = new Socket(addr.getHostString(), addr.getPort());
    sendTCPPacket(json, socket);
    return socket;
  }


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


  @SuppressWarnings("unchecked")
  public static void multicastTCP(GNSNodeConfig<String> gnsNodeConfig,
          Set<String> nameServerIds, JSONObject json, int numRetry,
          OldHackyConstants.PortType portType, Set<Object> excludeNameServers) {
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


  public static String getPacketTypeStringSafe(JSONObject json) {
    try {
      return getPacketType(json).toString();
      //}
    } catch (JSONException e) {
      return "Unknown";
    }
  }
}
