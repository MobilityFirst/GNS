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

import edu.umass.cs.gigapaxos.interfaces.NearestServerSelector;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestCallback;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnscommon.packets.ResponsePacket;
import edu.umass.cs.gnsserver.gnsapp.packet.Packet;
import edu.umass.cs.nio.AbstractJSONPacketDemultiplexer;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.reconfiguration.ReconfigurableAppClientAsync;
import edu.umass.cs.reconfiguration.ReconfigurationConfig.RC;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ActiveReplicaError;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.RequestActiveReplicas;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.Util;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Random;
import java.util.Set;
import java.util.logging.Level;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * Implements the <code>BasicPacketDemultiplexer</code> interface for using the nio package.
 *
 * @param <NodeIDType>
 */
public class LNSPacketDemultiplexer<NodeIDType> extends AbstractJSONPacketDemultiplexer {

  private final RequestHandlerInterface handler;
  private final Random random = new Random();

  final ReconfigurableAppClientAsync<Request> asyncLNSClient;

  /**
   * Create an instance of the LNSPacketDemultiplexer.
   *
   * @param handler
   * @param asyncClient
   */
  public LNSPacketDemultiplexer(RequestHandlerInterface handler, ReconfigurableAppClientAsync<Request> asyncClient) {
    this.handler = handler;
    this.asyncLNSClient = asyncClient;
    register(ReconfigurationPacket.PacketType.REQUEST_ACTIVE_REPLICAS);
    register(Packet.PacketType.COMMAND);
    register(Packet.PacketType.COMMAND_RETURN_VALUE);
  }

  private static final boolean USE_NEW_LNS_COMMAND_HANDLER = true; //false;

  /**
   * This is the entry point for all message received at a local name server.
   * It de-multiplexes packets based on their packet type and forwards to appropriate classes.
   *
   * @param json
   * @param header
   * @return false if an invalid packet type is received
   */
  @Override
  public boolean handleMessage(JSONObject json, NIOHeader header) {
    GNSConfig.getLogger().log(Level.INFO, ">>>>>>>>>>>>>>>>>>>>> Incoming packet: {0}", json);
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
            if (USE_NEW_LNS_COMMAND_HANDLER) {
              handleCommandPacket(json, header);
            } else {
              handleCommandPacketOld(json, header);
            }
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
      GNSConfig.getLogger().log(Level.WARNING, "Problem parsing packet from {0}: {1}", new Object[]{json, e});
    }

    return isPacketTypeFound;
  }

  private static boolean disableRequestActives = false;

  /**
   * If this is true we just send one copy to the nearest replica.
   */
  // FIXME: Remove this at some point.
  protected static boolean disableCommandRetransmitter = true;

  private RequestCallback callback = new RequestCallback() {

    @Override
    public void handleResponse(Request response) {
      try {
        LNSPacketDemultiplexer.this.handleCommandReturnValuePacket(
                response, null);
      } catch (JSONException | IOException e) {
        GNSConfig.getLogger().log(Level.WARNING,
                "Exception incurred upon receiving response: {0}", response);
        e.printStackTrace();
      }
    }
  };

  private NearestServerSelector redirector = new NearestServerSelector() {

    @Override
    public InetSocketAddress getNearest(Set<InetSocketAddress> servers) {
      return (handler).getClosestReplica(servers);
    }
  };

  /**
   * Handles a command packet that has come in from a client.
   *
   * @param json
   * @param header
   * @throws JSONException
   * @throws IOException
   */
  public void handleCommandPacket(JSONObject json, NIOHeader header) throws JSONException,
          IOException {

    CommandPacket packet = new CommandPacket(json);
    LNSRequestInfo requestInfo = new LNSRequestInfo(packet.getRequestID(),
            packet, header.sndr);
    GNSConfig.getLogger().log(Level.INFO,
            "{0} inserting outgoing request {1} with header {2}",
            new Object[]{this, json,  header});
    handler.addRequestInfo(packet.getRequestID(), requestInfo, header);
    packet = removeSenderInfo(json);

    if (requestInfo.getCommandType().isCreateDelete()
            || requestInfo.getCommandType().isSelect()) {
//      if (GNSCommandProtocol.CREATE_DELETE_COMMANDS.contains(requestInfo.getCommandName())
//            || requestInfo.getCommandName().equals(GNSCommandProtocol.SELECT)) {
      this.asyncLNSClient.sendRequestAnycast(packet, callback);
    } else {
      this.asyncLNSClient.sendRequest(packet, callback, redirector);
    }
  }
  
  @SuppressWarnings("deprecation")
private static CommandPacket removeSenderInfo(JSONObject json) throws JSONException {
	  json.remove(MessageNIOTransport.SNDR_IP_FIELD);
	  json.remove(MessageNIOTransport.SNDR_PORT_FIELD);
	  return new CommandPacket(json);
  }

  /**
   * Handles a command packet that has come in from a client.
   *
   * @param json
   * @param header
   * @throws JSONException
   * @throws IOException
   */
  public void handleCommandPacketOld(JSONObject json, NIOHeader header) throws JSONException, IOException {

    CommandPacket packet = new CommandPacket(json);
    int requestId = random.nextInt();
//    packet.setLNSRequestId(requestId);
    // Squirrel away the host and port so we know where to send the command return value
    LNSRequestInfo requestInfo = new LNSRequestInfo(requestId, packet, null);
    handler.addRequestInfo(requestId, requestInfo, header);

    // Send it to the client command handler
    Set<InetSocketAddress> actives;
    if (!disableRequestActives) {
      actives = handler.getActivesIfValid(packet.getServiceName());
    } else {
      // arun
      Util.suicide("Should never get here");
      actives = handler.getReplicatedActives(packet.getServiceName());
    }
    if (actives != null) {
      if (!disableRequestActives) {
        GNSConfig.getLogger().log(Level.FINE,
                "Found actives in cache for {0}: {1}", new Object[]{packet.getServiceName(), actives});
      } else {
        GNSConfig.getLogger().log(Level.FINE,
                "** USING DEFAULT ACTIVES for {0}: {1}", new Object[]{packet.getServiceName(), actives});
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
  public void handleCommandReturnValuePacket(JSONObject json)
          throws JSONException, IOException {
    this.handleCommandReturnValuePacket(new ResponsePacket(json), json);
  }

  /**
   * Handles sending the results of a command packet back to the client. Passing
   * json as well for legacy reasons and to avoid an unnecessary toJSON call.
   *
   * @throws JSONException
   * @throws IOException
   */
  private void handleCommandReturnValuePacket(Request response,
          JSONObject json) throws JSONException, IOException {
    ResponsePacket returnPacket = response instanceof ResponsePacket ? (ResponsePacket) response
            : null;
    ActiveReplicaError error = response instanceof ActiveReplicaError ? (ActiveReplicaError) response
            : null;
    GNSConfig.getLogger().log(
            Level.INFO,
            "{0} received response {1}",
            new Object[]{
              this,
              returnPacket != null ? returnPacket : error.getSummary()});
    assert (returnPacket != null || error != null);
    long id = returnPacket != null ? returnPacket.getRequestID() : error.getRequestID();
    String serviceName = returnPacket != null ? returnPacket.getServiceName() : error.getServiceName();
    LNSRequestInfo sentInfo;
    GNSConfig.getLogger().log(Level.INFO, "{0} matching {1} with {2}",
            new Object[]{this, id + "", handler.getRequestInfo(id)});
    if ((sentInfo = handler.getRequestInfo(id)) != null) {
      // doublecheck that it is for the same service name
      if ((sentInfo.getServiceName()
              .equals(serviceName)
              || // arun: except when service name is special name
              (sentInfo.getServiceName().equals(Config
                      .getGlobalString(RC.SPECIAL_NAME))))) {
        // String serviceName = returnPacket.getServiceName();
        GNSConfig.getLogger().log(Level.INFO, "{0} about to remove {1}",
                new Object[]{this, id + ""});
        handler.removeRequestInfo(id);
        // update cache - if the service name isn't missing (invalid)
        // and if it is a READ command
        // FIXME: THIS ISN'T GOING TO WORK WITHOUT MORE INFO ABOUT THE
        // REQUEST
        if (!CommandPacket.BOGUS_SERVICE_NAME.equals(serviceName)
                && sentInfo.getCommandType().isRead()
                //&& sentInfo.getCommandName().equals(GNSCommandProtocol.READ)
                && returnPacket != null) {
          handler.updateCacheEntry(serviceName,
                  returnPacket.getReturnValue());
        }
        // send the response back
        GNSConfig.getLogger()
                .log(Level.FINE,
                        "<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< LNS IS SENDING VALUE BACK TO {0}: {1}",
                        new Object[]{
                          sentInfo.getHost() + ":"
                          + sentInfo.getPort(),
                          returnPacket != null ? returnPacket.getSummary() : error.getSummary()});
        handler.sendToClient(new InetSocketAddress(sentInfo.getHost(),
                sentInfo.getPort()), json != null ? json
                : returnPacket != null ? returnPacket.toJSONObject()
                        : error.toJSONObject());
      } else {
        GNSConfig.getLogger().log(Level.SEVERE,
                "Command response packet mismatch: {0} vs. {1}", 
                new Object[]{sentInfo.getServiceName(), returnPacket.getServiceName()});
      }
    } else {
      GNSConfig.getLogger().log(Level.FINE,
              "Duplicate response for {0}: {1}",
              new Object[]{id, json != null ? json : returnPacket != null
                                ? returnPacket.toJSONObject() : error.toJSONObject()});
    }
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName();
  }

  private void handleRequestActives(JSONObject json) {
    GNSConfig.getLogger().log(Level.FINE,
            ")))))))))))))))))))))))))))) REQUEST ACTIVES RECEIVED: {0}", json.toString());
    try {
      RequestActiveReplicas requestActives = new RequestActiveReplicas(json);
      if (requestActives.getActives() != null) {
        for (InetSocketAddress address : requestActives.getActives()) {
          GNSConfig.getLogger().log(Level.FINE, "ACTIVE ADDRESS HOST: {0}", address.toString());
        }
        // Update the cache so that request actives task will now complete
        handler.updateCacheEntry(requestActives.getServiceName(), requestActives.getActives());
      }
    } catch (JSONException e) {
      GNSConfig.getLogger().log(Level.SEVERE,
              "Problem parsing RequestActiveReplicas packet info from {0}: {1}", new Object[]{json, e});
    }

  }

}
