package edu.umass.cs.gns.nsdesign.packet;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.util.ConfigFileInfo;
import edu.umass.cs.gns.util.ThreadUtils;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.Set;

/**
 * Reliable UDP send and receive
 *
 * This could be further integrated with the Packet class as they both are doing similar things. - Westy
 */
public class Transport {

  public static int MAXPACKETSIZE = 1500; // for larger packets, we can use TCP : abhigyan. see method
                                          // sendToNS in LocalNameServer.

  public static final String T_PORT = "tR";
  
  /**
   * This stores the address of the sender so we can use the Transport class more generally,
   * that is without having to have HostIDs
   */
  public static final String T_ADDRESS = "tADD";

  int myID;

  int myPort;

  DatagramSocket socket;


  /**
   * Note: You can specify -1 for the ID and it will use IP addresses instead.
   * @param id
   * @param port
//   * @param t
   */
  public Transport(int id, int port) {
    this.myID = id;
    this.myPort = port;
    while (true) {
      try {
        socket = new DatagramSocket(port);
        break;
      } catch (SocketException e) {
        GNS.getLogger().severe("Unable to create Transport object on " + port + " due to socket issue: " + e.toString()
                + "\n... waiting 3 seconds and trying again.");
        ThreadUtils.sleep(3000);
      }
    }
  }

  /**
   * Note: You can specify -1 for the ID and it will use IP addresses instead.
   * @param myID1 
   */
  public Transport(int myID1) {
    this.myID = myID1;
    while (true) {
      try {
        socket = new DatagramSocket();
        break;
      } catch (SocketException e) {
        GNS.getLogger().severe("Unable to create Transport object on a wildcard port due to socket issue: " + e.toString()
                + "\n... waiting 3 seconds and trying again.");
        ThreadUtils.sleep(3000);
      }
    }
    this.myPort = socket.getLocalPort();
  }

  public void sendPacket(JSONObject json, int destID, GNS.PortType portType) throws JSONException {
    sendPacket(json, destID, Packet.getPort(destID, portType));
  }

  public void sendPacket(JSONObject json, int destID, int port) throws JSONException {
    // make a copy of JSON object
    InetAddress address = ConfigFileInfo.getIPAddress(destID);
    sendPacket(json, address, port);
  }

  public void sendPacket(JSONObject json, InetAddress address, int port) throws JSONException {

      try {
          Packet.sendUDPPacket(socket, json, address, port);
      } catch (IOException e) {
          e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }

  }

  public void sendPacketToAll(JSONObject json, GNS.PortType portType, Set<Integer> destIDs) throws JSONException {
    sendPacketToAll(json, portType, destIDs, -1);
  }

  public void sendPacketToAll(JSONObject json, GNS.PortType portType, Set<Integer> destIDs, int excludeNameServerId) throws JSONException {
    for (Integer id : destIDs) {
      if (id.intValue() == excludeNameServerId) {
        continue;
      }
      sendPacket(json, id, Packet.getPort(id, portType));
    }
  }

  public JSONObject readPacket() {
    while (true) {
      //GNRS.getLogger().finer("Starting read wait.");
      DatagramPacket packet = new DatagramPacket(new byte[MAXPACKETSIZE], MAXPACKETSIZE);

      try {
        socket.receive(packet);
        //GNRS.getLogger().finer("Socket received packet.");
      } catch (IOException e) {
        GNS.getLogger().severe("IO Exception in receiving packet. Error msg: " + e.getMessage());
        e.printStackTrace();
      }

      JSONObject json;
      try {
        json = new JSONObject(new String(packet.getData()));
        //GNRS.getLogger().finer("Created json object. " + json.toString());
        // record this so we can send to the return address
        json.put(T_ADDRESS, packet.getAddress().getHostAddress());
        json.put(T_PORT, packet.getPort());
      } catch (JSONException e) {
        GNS.getLogger().severe("Error constructing JSONObject from packet.");
        e.printStackTrace();
        continue;
      }
        return json;
    }
  }

  
  public static String getReturnAddress(JSONObject json) {
    try {
      return json.getString(T_ADDRESS);
    } catch (JSONException e) {
      GNS.getLogger().finer("Error extracting return address from JSON encoded packet." + e);
      return "127.0.0.1";
    }
  }

  public static int getReturnPort(JSONObject json) {
    try {
      return json.getInt(T_PORT);
    } catch (JSONException e) {
      GNS.getLogger().finer("Error extracting return port from JSON encoded packet." + e);
      return -1;
    }
  }
}