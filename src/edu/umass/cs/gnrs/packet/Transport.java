package edu.umass.cs.gnrs.packet;

import edu.umass.cs.gnrs.main.GNS;
import edu.umass.cs.gnrs.util.ConfigFileInfo;
import edu.umass.cs.gnrs.util.ThreadUtils;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Reliable UDP send and receive
 *
 * This could be further integrated with the Packet class as they both are doing similar things. - Westy
 */
public class Transport {

  public static int MAXPACKETSIZE = 15000; // 1500 isn't big enough - data lengths of records will be bigger than 1500 - Westy
  public static int MAXRETRANSMISSIONS = 2;
  public static int TIMEOUT_MS = 500;
  public static int RECENTPACKETSIZE = 1000;
  public static final String T_PACKETID = "tP";
  public static final String T_ACK = "tA";
  public static final String T_HOSTID = "tH";
  public static final String T_PORT = "tR";
  
  /**
   * This stores the address of the sender so we can use the Transport class more generally,
   * that is without having to have HostIDs
   */
  public static final String T_ADDRESS = "tADD";
  //
  //
  int myID;
  int myPort;
  DatagramSocket socket;
  ConcurrentHashMap<Integer, Integer> sentPackets = new ConcurrentHashMap<Integer, Integer>();
//  LinkedList<Integer> recvdPacketsQueue = new LinkedList<Integer>();
//  HashSet<Integer> recvdPackets = new HashSet<Integer>();
  Random r = new Random();
  Timer timer;

  /**
   * Note: You can specify -1 for the ID and it will use IP addresses instead.
   * @param myID1
   * @param myPort1 
   */
  public Transport(int myID1, int myPort1) {
    this(myID1, myPort1, new Timer());
  }

  /**
   * Note: You can specify -1 for the ID and it will use IP addresses instead.
   * @param myID1
   * @param myPort1
   * @param t 
   */
  public Transport(int myID1, int myPort1, Timer t) {
    this.myID = myID1;
    this.myPort = myPort1;
    while (true) {
      try {
        socket = new DatagramSocket(myPort1);
        break;
      } catch (SocketException e) {
        GNS.getLogger().severe("Unable to create Transport object due to socket issue: " + e.toString()
                + "\n... waiting 3 seconds and trying again.");
        ThreadUtils.sleep(3000);
      }
    }
    this.timer = t;
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
        GNS.getLogger().severe("Unable to create Transport object due to socket issue: " + e.toString()
                + "\n... waiting 3 seconds and trying again.");
        ThreadUtils.sleep(3000);
      }
    }
    this.myPort = socket.getLocalPort();
    this.timer = new Timer();
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

//    JSONObject json = new JSONObject(originalJSON.toString());
    int packetID = r.nextInt();
    //GNRS.getLogger().finer("Sending packet. packetID " + packetID + " destID " + address);
    json.put(T_PACKETID, packetID);
    json.put(T_ACK, false);
    json.put(T_HOSTID, myID);
    json.put(T_PORT, myPort);

      try {
          Packet.sendUDPPacket(socket, json, address, port);
      } catch (IOException e) {
          e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }

      // Abhigyan: commenting this to reduce overhead
//      //GNRS.getLogger().finer("Prepared packet. packet " + json.toString());
//    sentPackets.put(packetID, packetID);
////    GNRS.getLogger().finer("Added packet to cache. " + sentPackets.toString());
//    SendPacket sendPacket = new SendPacket(packetID, json, address, port, this);
//    timer.schedule(sendPacket, 0, TIMEOUT_MS);
//    //GNRS.getLogger().finer("Timer scheduled. ");
      // Abhigyan: end of commented code
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

  // phasing out this version... use case is too verbose.. ports are always the same type
  public void sendPacketToAll(JSONObject json, ArrayList<Integer> destIDs, ArrayList<Integer> portNumbers) throws JSONException {
    //GNRS.getLogger().finer("Sending packet to all. " + destIDs);
    for (int i = 0; i < destIDs.size(); i++) {
      sendPacket(json, destIDs.get(i), portNumbers.get(i));
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
      } catch (JSONException e) {
        GNS.getLogger().severe("Error constructing JSONObject from packet.");
        e.printStackTrace();
        continue;
      }
        return json;
        // Abhigyan: Commenting rest of code to reduce overhead
//      int packetID = 0;
//      try {
//        // special case: packet not sent by Transport.java so don't send ACK.
//        if (!json.has(T_HOSTID) && !json.has(T_ACK)) {
//          //GNRS.getLogger().finer("Special case: Not sending ack. JSON:  " + json.toString());
//          return json;
//        }
//        packetID = json.getInt(T_PACKETID);
//        //GNRS.getLogger().finer("PacketID is " + packetID);
//        if (json.getBoolean(T_ACK)) {
//          //GNRS.getLogger().finer("*******GOLD GOLD GOLD ******* ACK Received " + packetID);
//          if (sentPackets.containsKey(packetID)) {
//            sentPackets.remove(packetID);
//            //GNRS.getLogger().finer("Removed Packet ID from sent packets. " + packetID);
//          }
//          continue;
//        }
//      } catch (JSONException e) {
//        GNRS.getLogger().severe("Error reading fields from JSON Object. Error msg: " + e.getMessage()
//                + " JSON Object:" + json.toString());
//        e.printStackTrace();
//        continue;
//      }
//      try {
//        sendAck(json);
//        //GNRS.getLogger().finer("Sent ACK for packet. " + packetID);
//      } catch (JSONException e) {
//        GNRS.getLogger().severe("JSONException in sending Ack for packet. Error msg: " + e.getMessage()
//                + " JSON Object " + json.toString());
//        e.printStackTrace();
//      } catch (IOException e) {
//        GNRS.getLogger().severe("IO Exception in sending ACK. Error msg: " + e.getMessage());
//        e.printStackTrace();
//      }
//      if (recvdPackets.contains(packetID)) {
//        //GNRS.getLogger().finer("Duplicate packet received. " + packetID);
//        continue;
//      } else {
//        if (recvdPackets.size() >= RECENTPACKETSIZE) {
//          int removedPacketID = recvdPacketsQueue.remove();
//          recvdPackets.remove(removedPacketID);
//          //GNRS.getLogger().finer("Remove packet IDs from cache. " + removedPacketID);
//        }
//        recvdPackets.add(packetID);
//        recvdPacketsQueue.add(packetID);
//        //GNRS.getLogger().finer("Insert packet IDs into packet cache. " + packetID);
//        return json;
//      }
        // Abhigyan: end of commented code

    }
  }

  private void sendAck(JSONObject json) throws JSONException, IOException {
    JSONObject jsonAck = new JSONObject();
    jsonAck.put(T_PACKETID, json.getInt(T_PACKETID));
    jsonAck.put(T_ACK, true);
    //GNRS.getLogger().finer("Ack packet prepared. " + jsonAck.toString());
    int hostID = json.getInt(T_HOSTID);
    InetAddress add;
    if (hostID == -1) {
      add = InetAddress.getByName(json.getString(T_ADDRESS));
    } else {
      add = ConfigFileInfo.getIPAddress(hostID);
    }
    int port = json.getInt(T_PORT);
    //GNRS.getLogger().finer("Address : " + add.toString());
    //GNRS.getLogger().finer("Port : " + port);
    Packet.sendUDPPacket(socket, jsonAck, add, port);
    //GNRS.getLogger().info("Ack packet sent. to " + add + ":" + port);
    //GNRS.getLogger().finer("Packet sent. to " + json.getInt(T_HOSTID) + " at port " + port);
  }
  
  public static String getReturnAddress(JSONObject json) {
    try {
      return json.getString(T_ADDRESS);
    } catch (JSONException e) {
      GNS.getLogger().fine("Error extracting return address from JSON encoded packet." + e);
      return "127.0.0.1";
    }
  }

  public static int getReturnPort(JSONObject json) {
    try {
      return json.getInt(T_PORT);
    } catch (JSONException e) {
      GNS.getLogger().fine("Error extracting return port from JSON encoded packet." + e);
      return -1;
    }
  }

// test code
//  public static void main(String[] args) throws Exception {
//    Transport transport = new Transport(0);
//    System.out.println(transport.socket.getInetAddress() + " " + transport.socket.getLocalAddress());
//    System.out.println(InetAddress.getLocalHost());
//  }
}

//class SendPacket extends TimerTask {
//
//  int packetID;
//  JSONObject json;
//  InetAddress address;
//  int port;
//  int numberTransmissions;
//  Transport transport;

//  public SendPacket(int packetID, JSONObject json, InetAddress address, int port, Transport transport) {
//    this.packetID = packetID;
//    this.json = json;
//    this.address = address;
//    this.port = port;
//    this.numberTransmissions = 0;
//    this.transport = transport;
//
//  }
//
//  @Override
//  public void run() {
//
//    //GNRS.getLogger().finer("Timer wake up.");
//    if (transport.sentPackets.contains(packetID)) {
//      //GNRS.getLogger().finer("Found packet in send packets. PacketID " + packetID);
//      numberTransmissions++;
//      if (numberTransmissions > Transport.MAXRETRANSMISSIONS + 1) {
//        GNRS.getLogger().fine("Transmissions exceeded. Cancel Timer: PacketID " + packetID
//                + " Number transmissions: " + numberTransmissions);
//        transport.sentPackets.remove(packetID);
//        this.cancel();
//        return;
//      }
//      //GNRS.getLogger().finer("Number of transmissions. " + numberTransmissions + " " + packetID);
//      try {
//        Packet.sendUDPPacket(transport.socket, json, address, port);
//        //GNRS.getLogger().info("Sent packet id = " + packetID + " address = " + address + " port = " + port);
//        //GNRS.getLogger().finer("Sent packet. PacketID " + packetID);
//      } catch (IOException e) {
//        GNRS.getLogger().severe("IOException in sending UDP Packet. ID: " + packetID + " Error msg: " + e.getMessage());
//        e.printStackTrace();
//      }
//    } else {
//      this.cancel();
//      //GNRS.getLogger().finer("Ack received for packet. Cancel Timer: PacketID " + packetID + " Number transmissions: " + numberTransmissions);
//      return;
//    }
//  }
//}
