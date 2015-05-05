/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.gns.nio.JSONNIOTransport;
import edu.umass.cs.gns.nsdesign.packet.CommandPacket;
import edu.umass.cs.gns.nsdesign.packet.CommandValueReturnPacket;
import edu.umass.cs.gns.nsdesign.packet.Packet;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Implements the <code>BasicPacketDemultiplexer</code> interface for using the {@link edu.umass.cs.gns.nio} package.
 *
 * @param <NodeIDType>
 */
public class LNSPacketDemultiplexer<NodeIDType> extends AbstractPacketDemultiplexer {

  LocalNameServer lns;

  private static final ConcurrentMap<Integer, LNSCommandInfo> outstandingRequests = new ConcurrentHashMap<>(10, 0.75f, 3);

  private final boolean debuggingEnabled = true;

  public LNSPacketDemultiplexer(LocalNameServer lns) {
    this.lns = lns;
    register(Packet.PacketType.COMMAND);
    register(Packet.PacketType.COMMAND_RETURN_VALUE);
  }

  /**
   * This is the entry point for all message received at a local name server.
   * It de-multiplexes packets based on their packet type and forwards to appropriate classes.
   *
   * @param json
   * @return false if an invalid packet type is received
   */
  @Override
  public boolean handleJSONObject(JSONObject json) {
    if (debuggingEnabled) {
      GNS.getLogger().info("#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#>>>>>>>>> Incoming packet: " + json);
    }
    boolean isPacketTypeFound = true;
    try {
      switch (Packet.getPacketType(json)) {
        case COMMAND:
          handleCommandPacket(json);
          break;
        case COMMAND_RETURN_VALUE:
          handleCommandReturnValuePacket(json);
          break;
        default:
          isPacketTypeFound = false;
          break;
      }
    } catch (JSONException | IOException e) {
      e.printStackTrace();
    }
    return isPacketTypeFound;
  }

  public void handleCommandPacket(JSONObject json) throws JSONException, IOException {
    CommandPacket packet = new CommandPacket(json);
    // Squirrel away the host and port so we know where to send the command return value
    outstandingRequests.put(packet.getRequestId(), new LNSCommandInfo(packet.getSenderAddress(), packet.getSenderPort()));
    // remove these so the stamper will put new ones in so the packet will find it's way back here
    json.remove(JSONNIOTransport.DEFAULT_IP_FIELD);
    json.remove(JSONNIOTransport.DEFAULT_PORT_FIELD);
    // Send it to the client command handler
    Object node = lns.getNodeConfig().getClosestServer(lns.getNodeConfig().getActiveReplicas());
    lns.getTcpTransport().sendToID(node, json);
  }

  public void handleCommandReturnValuePacket(JSONObject json) throws JSONException, IOException {
    CommandValueReturnPacket returnPacket = new CommandValueReturnPacket(json, lns.getNodeConfig());
    int id = returnPacket.getRequestId();
    LNSCommandInfo sentInfo;
    if ((sentInfo = outstandingRequests.get(id)) != null) {
      outstandingRequests.remove(id);
      if (debuggingEnabled) {
        GNS.getLogger().info("#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#>>>>>>>>> LNS IS SENDING VALUE BACK TO "
                + sentInfo.getHost() + "/" + sentInfo.getPort() + ": " + returnPacket.toString());
      }
      lns.getTcpTransport().sendToAddress(new InetSocketAddress(sentInfo.getHost(), sentInfo.getPort()),
              json);
    } else {
      GNS.getLogger().severe("Command packet info not found for " + id + ": " + json);
    }
  }

}
