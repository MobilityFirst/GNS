/*
 * Copyright (C) 2015
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.GnsProtocolDefs;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.newApp.packet.CommandPacket;
import edu.umass.cs.gns.newApp.packet.CommandValueReturnPacket;
import edu.umass.cs.gns.newApp.packet.Packet;
import edu.umass.cs.nio.AbstractJSONPacketDemultiplexer;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.RequestActiveReplicas;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Random;
import java.util.Set;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * Implements the <code>BasicPacketDemultiplexer</code> interface for using the {@link edu.umass.cs.gns.nio} package.
 *
 * @param <NodeIDType>
 */
public class LNSPacketDemultiplexer<NodeIDType> extends AbstractJSONPacketDemultiplexer {

  private final RequestHandlerInterface handler;
  private final Random random = new Random();

  public LNSPacketDemultiplexer(RequestHandlerInterface handler) {
    this.handler = handler;
    register(ReconfigurationPacket.PacketType.REQUEST_ACTIVE_REPLICAS);
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
  public boolean handleMessage(JSONObject json) {
    if (handler.isDebugMode()) {
      GNS.getLogger().info(">>>>>>>>>>>>>>>>>>>>> Incoming packet: " + json);
    }
    boolean isPacketTypeFound = true;
    try {
      if (ReconfigurationPacket.isReconfigurationPacket(json)) {
        switch (ReconfigurationPacket.getReconfigurationPacketType(json)) {
          case REQUEST_ACTIVE_REPLICAS:
            handleRequestActives(json);
            break;
          default:
            isPacketTypeFound = false;
            break;
        }
      } else {
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
      }
    } catch (JSONException | IOException e) {
      GNS.getLogger().warning("Problem parsing packet from " + json + ": " + e);
    }

    return isPacketTypeFound;
  }

  private static boolean disableRequestActives = true;
  protected static boolean useCommandRetransmitter = false;

  public void handleCommandPacket(JSONObject json) throws JSONException, IOException {

    CommandPacket packet = new CommandPacket(json);
    int requestId = random.nextInt();
    packet.setLNSRequestId(requestId);
    // Squirrel away the host and port so we know where to send the command return value
    LNSRequestInfo requestInfo = new LNSRequestInfo(requestId, packet);
    handler.addRequestInfo(requestId, requestInfo);

    // Send it to the client command handler
    Set<InetSocketAddress> actives;
    if (!disableRequestActives) {
      actives = handler.getActivesIfValid(packet.getServiceName());
    } else {
      actives = handler.getNodeConfig().getReplicatedActives(packet.getServiceName());
    }
    if (actives != null) {
      if (handler.isDebugMode()) {
        if (!disableRequestActives) {
          GNS.getLogger().info("Found actives in cache for " + packet.getServiceName() + ": " + actives);
        } else {
          GNS.getLogger().info("** USING DEFAULT ACTIVES for " + packet.getServiceName() + ": " + actives);
        }
      }
      if (!useCommandRetransmitter) {
        handler.sendToClosestServer(actives, packet.toJSONObject());
      } else {
        handler.getProtocolExecutor().schedule(new CommandRetransmitter(requestId, packet.toJSONObject(),
                actives, handler));
      }

    } else {
      handler.getProtocolExecutor().schedule(new RequestActives(requestInfo, handler));
    }
  }

  public void handleCommandReturnValuePacket(JSONObject json) throws JSONException, IOException {
    CommandValueReturnPacket returnPacket = new CommandValueReturnPacket(json);
    int id = returnPacket.getLNSRequestId();
    LNSRequestInfo sentInfo;
    if ((sentInfo = handler.getRequestInfo(id)) != null) {
      // doublecheck that it is for the same service name
      if (sentInfo.getServiceName().equals(returnPacket.getServiceName())) {
        String serviceName = returnPacket.getServiceName();
        handler.removeRequestInfo(id);
        // update cache - if the service name isn't missing (invalid)
        // and if it is a READ command
        // FIXME: THIS ISN'T GOING TO WORK WITHOUT MORE INFO ABOUT THE REQUEST
        if (!CommandPacket.BOGUS_SERVICE_NAME.equals(serviceName)
                && sentInfo.getCommandType().equals(GnsProtocolDefs.NEWREAD)) {
          handler.updateCacheEntry(serviceName, returnPacket.getReturnValue());
        }
        // send the response back
        if (handler.isDebugMode()) {
          GNS.getLogger().info("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< LNS IS SENDING VALUE BACK TO "
                  + sentInfo.getHost() + "/" + sentInfo.getPort() + ": " + returnPacket.toString());
        }
        handler.getTcpTransport().sendToAddress(new InetSocketAddress(sentInfo.getHost(), sentInfo.getPort()),
                json);
      } else {
        GNS.getLogger().severe("Command response packet mismatch: " + sentInfo.getServiceName()
                + " vs. " + returnPacket.getServiceName());
      }
    } else {
      if (handler.isDebugMode()) {
        GNS.getLogger().info("Duplicate response for " + id + ": " + json);
      }
    }
  }

  private void handleRequestActives(JSONObject json) {
    if (handler.isDebugMode()) {
      GNS.getLogger().info(")))))))))))))))))))))))))))) REQUEST ACTIVES RECEIVED: " + json.toString());

    }
    try {
      RequestActiveReplicas requestActives = new RequestActiveReplicas(json);
      if (requestActives.getActives() != null) {
        if (handler.isDebugMode()) {
          for (InetSocketAddress address : requestActives.getActives()) {
            GNS.getLogger().info("ACTIVE ADDRESS HOST: " + address.getHostString());
          }
        }
        // Update the cache so that request actives task will now complete
        handler.updateCacheEntry(requestActives.getServiceName(), requestActives.getActives());
        // also update the set of the nodes the ping manager is using
        handler.getPingManager().addActiveReplicas(requestActives.getActives());
      }
    } catch (JSONException e) {
      GNS.getLogger().severe("Problem parsing RequestActiveReplicas packet info from " + json + ": " + e);
    }

  }

}
