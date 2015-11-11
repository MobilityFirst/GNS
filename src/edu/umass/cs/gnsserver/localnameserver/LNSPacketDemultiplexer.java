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
package edu.umass.cs.gnsserver.localnameserver;

import edu.umass.cs.gnscommon.GnsProtocol;
import edu.umass.cs.gnsserver.main.GNS;
import edu.umass.cs.gnsserver.gnsApp.packet.CommandPacket;
import edu.umass.cs.gnsserver.gnsApp.packet.CommandValueReturnPacket;
import edu.umass.cs.gnsserver.gnsApp.packet.Packet;
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
 * Implements the <code>BasicPacketDemultiplexer</code> interface for using the {@link edu.umass.cs.nio} package.
 *
 * @param <NodeIDType>
 */
public class LNSPacketDemultiplexer<NodeIDType> extends AbstractJSONPacketDemultiplexer {

  private final RequestHandlerInterface handler;
  private final Random random = new Random();

  /**
   * Create an instance of the LNSPacketDemultiplexer.
   * 
   * @param handler
   */
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

  private static boolean disableRequestActives = false;

  /**
   * If this is true we just send one copy to the nearest replica. 
   */
  // FIXME: Remove this at some point.
  protected static boolean disableCommandRetransmitter = true;

  /**
   * Handles a command packet that has come in from a client.
   * 
   * @param json
   * @throws JSONException
   * @throws IOException
   */
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
      actives = handler.getReplicatedActives(packet.getServiceName());
    }
    if (actives != null) {
      if (handler.isDebugMode()) {
        if (!disableRequestActives) {
          GNS.getLogger().info("Found actives in cache for " + packet.getServiceName() + ": " + actives);
        } else {
          GNS.getLogger().info("** USING DEFAULT ACTIVES for " + packet.getServiceName() + ": " + actives);
        }
      }
      if (disableCommandRetransmitter) {
        handler.sendToClosestReplica(actives, packet.toJSONObject());
      } else {
        handler.getProtocolExecutor().schedule(new CommandRetransmitter(requestId, packet.toJSONObject(),
                actives, handler));
      }

    } else {
      handler.getProtocolExecutor().schedule(new RequestActives(requestInfo, handler));
    }
  }

  /**
   * Handles sending the results of a command packet back to the client.
   * 
   * @param json
   * @throws JSONException
   * @throws IOException
   */
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
                && sentInfo.getCommandType().equals(GnsProtocol.NEWREAD)) {
          handler.updateCacheEntry(serviceName, returnPacket.getReturnValue());
        }
        // send the response back
        if (handler.isDebugMode()) {
          GNS.getLogger().info("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< LNS IS SENDING VALUE BACK TO "
                  + sentInfo.getHost() + "/" + sentInfo.getPort() + ": " + returnPacket.toString());
        }
        handler.sendToClient(new InetSocketAddress(sentInfo.getHost(), sentInfo.getPort()), json);
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
