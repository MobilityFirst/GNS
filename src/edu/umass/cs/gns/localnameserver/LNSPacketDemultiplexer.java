/*
 * Copyright (C) 2015
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
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
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Implements the <code>BasicPacketDemultiplexer</code> interface for using the {@link edu.umass.cs.gns.nio} package.
 *
 * @param <NodeIDType>
 */
public class LNSPacketDemultiplexer<NodeIDType> extends AbstractPacketDemultiplexer {

  RequestHandlerInterface handler;

  public LNSPacketDemultiplexer(RequestHandlerInterface handler) {
    this.handler = handler;
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
    if (handler.isDebugMode()) {
      GNS.getLogger().info(">>>>>>>>>>>>>>>>>>>>> Incoming packet: " + json);
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
    handler.addRequestInfo(packet.getRequestId(), 
            new LNSRequestInfo(packet.getRequestId(), 
                    packet.getServiceName(),
                    packet.getSenderAddress(), packet.getSenderPort()));
    // remove these so the stamper will put new ones in so the packet will find it's way back here
    json.remove(JSONNIOTransport.DEFAULT_IP_FIELD);
    json.remove(JSONNIOTransport.DEFAULT_PORT_FIELD);
    // Send it to the client command handler
    InetSocketAddress address = handler.getClosestServer(handler.getNodeConfig().getActiveReplicas());
    handler.getTcpTransport().sendToAddress(address, json);
  }

  public void handleCommandReturnValuePacket(JSONObject json) throws JSONException, IOException {
    CommandValueReturnPacket returnPacket = new CommandValueReturnPacket(json);
    int id = returnPacket.getRequestId();
    LNSRequestInfo sentInfo;
    if ((sentInfo = handler.getRequestInfo(id)) != null) {
      handler.removeRequestInfo(id);
      if (handler.isDebugMode()) {
        GNS.getLogger().info("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< LNS IS SENDING VALUE BACK TO "
                + sentInfo.getHost() + "/" + sentInfo.getPort() + ": " + returnPacket.toString());
      }
      handler.getTcpTransport().sendToAddress(new InetSocketAddress(sentInfo.getHost(), sentInfo.getPort()),
              json);
    } else {
      GNS.getLogger().severe("Command packet info not found for " + id + ": " + json);
    }
    
  }

}
