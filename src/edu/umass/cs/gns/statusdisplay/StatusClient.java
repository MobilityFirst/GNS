/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.umass.cs.gns.statusdisplay;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.nodeconfig.NodeId;
import edu.umass.cs.gns.nsdesign.packet.Packet;
import edu.umass.cs.gns.nsdesign.packet.admin.StatusPacket;
import edu.umass.cs.gns.nsdesign.packet.admin.TrafficStatusPacket;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Set;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class StatusClient {

  private static InetAddress statusServerAddress = null;
  private static boolean warningIssued = false;
  private static int problemCount = 0;
  private static final int MAXPROBLEMS = 10; // after this we ignore status send calls

  public static InetAddress getStatusServerAddress() {
    return statusServerAddress;
  }

  public static void setStatusServerAddress(InetAddress address) {
    StatusClient.statusServerAddress = address;
    // reset these when we toString a new address
    problemCount = 0;
    warningIssued = false;
  }

  public static synchronized void sendStatus(NodeId<String> id, String message) {
    if (statusServerAddress == null) {
      if (!warningIssued) {
        GNS.getLogger().warning("Unable to send status. Server address has not been initialized!");
        warningIssued = true;
      }
      return;
    }
    // just in case things go south
    if (problemCount > MAXPROBLEMS) {
      return;
    }
    JSONObject jsonObject = new JSONObject();
    try {
      jsonObject.put(StatusListener.MESSAGE, message);
    } catch (JSONException e) {
      GNS.getLogger().severe("Unable to put JSON field on object: " + e);
      problemCount++;
      return;
    }
    StatusPacket packet = new StatusPacket(id, jsonObject);
    try {
      JSONObject json = packet.toJSONObject();
      Socket socketTCP = new Socket(statusServerAddress, StatusListener.PORT);
      Packet.sendTCPPacket(json, socketTCP);
      socketTCP.close();

    } catch (JSONException e) {
      GNS.getLogger().severe("Unable to send status: " + e + ". No worries, this will be ignored after " + (MAXPROBLEMS - problemCount) + " more attempts.");
      problemCount++;
    } catch (IOException e) {
      GNS.getLogger().severe("Unable to send status: " + e + ". No worries, this will be ignored after " + (MAXPROBLEMS - problemCount) + " more attempts.");
      problemCount++;
    }
  }

  /**
   * Sends a message to the status server indicating traffic from fromID to all the toIDs.
   *
   * @param fromID
   * @param toIDs
   * @param portType
   * @param packetType
   */
  public static void sendTrafficStatus(NodeId<String> fromID, Set<NodeId<String>> toIDs, GNS.PortType portType, Packet.PacketType packetType) {
    sendTrafficStatus(fromID, toIDs, new NodeId<String>(-1), portType, packetType);
  }
  
  /**
   * Sends a message to the status server indicating traffic from fromID to all the toIDs.
   * name and key specify the relevant GNRS record.
   * 
   * @param fromID
   * @param toIDs
   * @param portType
   * @param packetType
   * @param name
   * @param key 
   */
  public static void sendTrafficStatus(NodeId<String> fromID, Set<NodeId<String>> toIDs, GNS.PortType portType, Packet.PacketType packetType, 
          String name) {
    sendTrafficStatus(fromID, toIDs, new NodeId<String>(-1), portType, packetType, name);
  }

  /**
   * Sends a message to the status server indicating traffic from fromID to all the toIDs minus the one in exclude.
   *
   * @param fromID
   * @param toIDs
   * @param exclude
   * @param portType
   * @param packetType
   */
  public static void sendTrafficStatus(NodeId<String> fromID, Set<NodeId<String>> toIDs, NodeId<String> exclude, GNS.PortType portType, Packet.PacketType packetType) {
    for (NodeId<String> id : toIDs) {
      if (!exclude.equals(id)) {
        StatusClient.sendTrafficStatus(fromID, id, portType, packetType);
      }
    }
  }
  
  /**
   * Sends a message to the status server indicating traffic from fromID to all the toIDs minus the one in exclude. 
   * name and key specify the relevant GNRS record.
   * 
   * @param fromID
   * @param toIDs
   * @param exclude
   * @param portType
   * @param packetType
   * @param name 
   */
  public static void sendTrafficStatus(NodeId<String> fromID, Set<NodeId<String>> toIDs, NodeId<String> exclude, GNS.PortType portType, 
          Packet.PacketType packetType,
          String name) {
    for (NodeId<String> id : toIDs) {
      if (!exclude.equals(id)) {
        StatusClient.sendTrafficStatus(fromID, id, portType, packetType, name);
      }
    }
  }

  /**
   * Sends a message to the status server indicating traffic from fromID to all the toIDs minus those on exclude.
   *
   * @param fromID
   * @param toIDs
   * @param exclude
   * @param portType
   * @param packetType
   */
  public static void sendTrafficStatus(NodeId<String> fromID, Set<NodeId<String>> toIDs, Set<NodeId<String>> exclude, GNS.PortType portType, Packet.PacketType packetType) {
    for (NodeId<String> id : toIDs) {
      if (!exclude.contains(id)) {
        StatusClient.sendTrafficStatus(fromID, id, portType, packetType);
      }
    }
  }
  
  /**
   * Sends a message to the status server indicating traffic from fromID to all the toIDs minus those on exclude. 
   * name and key specify the relevant GNRS record.
   * 
   * @param fromID
   * @param toIDs
   * @param exclude
   * @param portType
   * @param packetType
   * @param name
   * @param key 
   */
  public static void sendTrafficStatus(NodeId<String> fromID, Set<NodeId<String>> toIDs, Set<NodeId<String>> exclude, GNS.PortType portType, Packet.PacketType packetType,
          String name) {
    for (NodeId<String> id : toIDs) {
      if (!exclude.contains(id)) {
        StatusClient.sendTrafficStatus(fromID, id, portType, packetType, name);
      }
    }
  }

  /**
   * Sends a message to the status server indicating traffic from fromID to toID.
   *
   * @param fromID
   * @param toID
   * @param portType
   * @param packetType
   */
  public static synchronized void sendTrafficStatus(NodeId<String> fromID, NodeId<String> toID, GNS.PortType portType, Packet.PacketType packetType) {
    sendTrafficStatus(fromID, toID, portType, packetType, null, null);
  }

  /**
   * Sends a message to the status server indicating traffic from fromID to toID. name and key specify the 
   * relevant GNRS record.
   * 
   * @param fromID
   * @param toID
   * @param portType
   * @param packetType
   * @param name
   * @param key 
   */
  public static synchronized void sendTrafficStatus(NodeId<String> fromID, NodeId<String> toID, GNS.PortType portType, Packet.PacketType packetType,
          String name) {
    sendTrafficStatus(fromID, toID, portType, packetType, name, null);
  }
  
  /**
   * * Sends a message to the status server indicating traffic from fromID to toID. name and key specify the 
   * relevant GNRS record. other can be used for additional annotations.
   * @param fromID
   * @param toID
   * @param portType
   * @param packetType
   * @param name
   * @param key
   * @param other 
   */
  public static synchronized void sendTrafficStatus(NodeId<String> fromID, NodeId<String> toID, GNS.PortType portType, Packet.PacketType packetType,
          String name, String other) {
    if (statusServerAddress == null) {
      if (!warningIssued) {
        GNS.getLogger().warning("Unable to send traffic status. Server address has not been initialized!");
        warningIssued = true;
      }
      return;
    }
    // just in case things go south
    if (problemCount > MAXPROBLEMS) {
      return;
    }
    TrafficStatusPacket packet = new TrafficStatusPacket(fromID, toID, portType, packetType, name, other);
    try {
      JSONObject json = packet.toJSONObject();
      Socket socketTCP = new Socket(statusServerAddress, StatusListener.PORT);
      Packet.sendTCPPacket(json, socketTCP);
      socketTCP.close();

    } catch (JSONException e) {
      GNS.getLogger().severe("Unable to send status: " + e + ". No worries, this will be ignored after " + (MAXPROBLEMS - problemCount) + " more attempts.");
      problemCount++;
    } catch (IOException e) {
      GNS.getLogger().severe("Unable to send status: " + e + ". No worries, this will be ignored after " + (MAXPROBLEMS - problemCount) + " more attempts.");
      problemCount++;
    }
  }

  public static void handleStatusInit(InetAddress address) {
    StatusClient.setStatusServerAddress(address);
    GNS.getLogger().info("Setting Status Server Address to " + address.getHostAddress());
  }
}
