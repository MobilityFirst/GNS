/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.umass.cs.gns.statusdisplay;

import edu.umass.cs.gns.localnameserver.LocalNameServer;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nio.InterfaceNodeConfig;
import edu.umass.cs.gns.nsdesign.nodeconfig.GNSNodeConfig;
import edu.umass.cs.gns.nsdesign.packet.Packet;
import edu.umass.cs.gns.nsdesign.packet.admin.StatusInitPacket;
import edu.umass.cs.gns.nsdesign.packet.admin.StatusPacket;
import edu.umass.cs.gns.nsdesign.packet.admin.TrafficStatusPacket;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Date;
import java.util.Set;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class StatusListener extends Thread {

  public static final String MESSAGE = "message";
  public static final int PORT = 23999;
  private ServerSocket serverSocket;

  public StatusListener() throws IOException {
    super("StatusListener");
    this.serverSocket = new ServerSocket(PORT);
  }

  @Override
  public void run() {
    JSONObject json;
    GNS.getLogger().info("Starting Status Server on port " + serverSocket.getLocalPort());
    // now we start listening
    while (true) {
      Socket socket;
      try {
        socket = serverSocket.accept();
        //Read the packet from the input stream
        json = Packet.getJSONObjectFrame(socket);
      } catch (IOException e) {
        GNS.getLogger().warning("Ignoring error accepting socket connection: " + e);
        e.printStackTrace();
        continue;
      } catch (JSONException e) {
        GNS.getLogger().warning("Unable to parse json from packet: " + e);
        continue;
      }
      System.out.println("Incoming status packet: " + json.toString());
      try {
        switch (Packet.getPacketType(json)) {
          case STATUS:

            StatusPacket statusPacket = new StatusPacket(json);
            Date time = statusPacket.getTime();
            JSONObject jsonObject = statusPacket.getJsonObject();
            String message = jsonObject.optString(MESSAGE);
            if (message != null) {
              StatusModel.getInstance().queueUpdate(statusPacket.getId(), message, time);
              //StatusModel.getInstance().updateEntry(statusPacket.getId(), message, time);
            }
            break;
          case TRAFFIC_STATUS:
            //TrafficStatusPacket pkt = new TrafficStatusPacket(json);
            StatusModel.getInstance().queueSendNotation(new TrafficStatusPacket(json));
            //StatusModel.getInstance().addSendNotation(new TrafficStatusPacket(json));
            //StatusModel.getInstance().addSendNotation(pkt.getFromID(), pkt.getToID(), pkt.getPortType(), pkt.getPacketType(), pkt.getTime());
            break;
        }
      } catch (Exception e) {
        GNS.getLogger().warning("Ignoring error handling packets: " + e);
        e.printStackTrace();
      }
      //System.out.println(StatusModel.getInstance().toString());
      try {
        socket.close();
      } catch (IOException e) {
        GNS.getLogger().warning("Error closing socket: " + e);
        e.printStackTrace();
      }
    }
  }

  /**
   * Tells the remote hosts where to send their status packets. This should be called after all the remote hosts are
   * running.
   */
  public static void sendOutServerInitPackets(GNSNodeConfig nodeConfig, Set<Integer> ids) {
    JSONObject json;
    System.out.println("Sending status server init to all servers");
    // First we tell them all where we are
    StatusInitPacket packet = new StatusInitPacket();
    try {
      json = packet.toJSONObject();
    } catch (JSONException e) {
      System.out.println("Unable to convert packet to a json object: " + e);
      return;
    }


    for (int id : ids) {
      // send out the StatusInit packet to all local name servers
      System.out.println("Sending status server init to LNS " + id);
      try {
        Packet.sendTCPPacket(json, new InetSocketAddress(LocalNameServer.getAddress().getHostString(), GNS.DEFAULT_LNS_ADMIN_PORT));
      } catch (IOException e) {
         System.out.println("Error sending status init to LNS " + id + " : " + e);
      }
      // send out the StatusInit packet to all name servers
      System.out.println("Sending status server init to NS " + id);
      try {
        Packet.sendTCPPacket(nodeConfig, json, id, GNS.PortType.NS_ADMIN_PORT);
      } catch (IOException e) {
         System.out.println("Error sending status init to NS " + id + " : " + e);
      }
    }

  }
//  /**
//   * Tells the remote hosts where to send their status packets. This should be called after all the remote hosts are
//   * running.
//   */
//  public static void sendOutServerInitPacketsOld(Set<String> hosts) {
//    JSONObject json;
//    System.out.println("Sending status server init to all servers");
//    // First we tell them all where we are
//    StatusInitPacket packet = new StatusInitPacket();
//    try {
//      json = packet.toJSONObject();
//    } catch (JSONException e) {
//      System.out.println("Unable to convert packet to a json object: " + e);
//      return;
//    }
//
//    try {
//      for (String host : hosts) {
//        // send out the StatusInit packet to all local name servers - currently only works if the LNS is running on the same machine as an NS
//        System.out.println("Sending status server init to NS " + host + ":" + GNRS.startingPort + GNRS.PortType.LNS_ADMIN_PORT.getOffset());
//        Packet.sendTCPPacket(json, new Socket(host, (GNRS.startingPort + GNRS.PortType.LNS_ADMIN_PORT.getOffset())));   
//        // send out the StatusInit packet to all name servers
//        System.out.println("Sending status server init to NS " + host + ":" + GNRS.startingPort + GNRS.PortType.NS_ADMIN_PORT.getOffset() );
//        Packet.sendTCPPacket(json, new Socket(host, (GNRS.startingPort + GNRS.PortType.NS_ADMIN_PORT.getOffset())));
//      }
//    } catch (IOException e) {
//      System.out.println("Unable to send Status init packet: " + e);
//    }
//  }
//  public static void main(String[] args) throws IOException {
//    String runSetName = EC2Installer.currentRunSetName();
//    Set<String> hosts = EC2Installer.runSetHosts.get(runSetName);
//    sendOutServerInitPackets(hosts);
//    new StatusListener().run();
//  }
}
