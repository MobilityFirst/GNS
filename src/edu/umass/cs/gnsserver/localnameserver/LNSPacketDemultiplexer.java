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
import edu.umass.cs.gnscommon.GnsProtocol;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.gnsserver.gnsapp.packet.CommandPacket;
import edu.umass.cs.gnsserver.gnsapp.packet.CommandValueReturnPacket;
import edu.umass.cs.gnsserver.gnsapp.packet.Packet;
import edu.umass.cs.nio.AbstractJSONPacketDemultiplexer;
import edu.umass.cs.nio.SSLDataProcessingWorker.SSL_MODES;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.ReconfigurableAppClientAsync;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.reconfiguration.ReconfigurationConfig.RC;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ActiveReplicaError;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.RequestActiveReplicas;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
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
 * Implements the <code>BasicPacketDemultiplexer</code> interface for using the {@link edu.umass.cs.nio} package.
 *
 * @param <NodeIDType>
 */
public class LNSPacketDemultiplexer<NodeIDType> extends AbstractJSONPacketDemultiplexer {

  private final RequestHandlerInterface handler;
  private final Random random = new Random();
  
  final ReconfigurableAppClientAsync asyncLNSClient;


  /**
   * Create an instance of the LNSPacketDemultiplexer.
   * 
   * @param handler
   */
  public LNSPacketDemultiplexer(RequestHandlerInterface handler, ReconfigurableAppClientAsync asyncClient) {
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
   * @return false if an invalid packet type is received
   */
  @Override
  public boolean handleMessage(JSONObject json) {
    if (handler.isDebugMode()) {
      GNSConfig.getLogger().info(">>>>>>>>>>>>>>>>>>>>> Incoming packet: " + json);
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
					if (USE_NEW_LNS_COMMAND_HANDLER)
						handleCommandPacket(json);
					else
						handleCommandPacketOld(json);
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
      GNSConfig.getLogger().warning("Problem parsing packet from " + json + ": " + e);
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
				GNSConfig.getLogger().warning(
						"Exception incurred upon receiving response: "
								+ response);
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
   * @throws JSONException
   * @throws IOException
   */
	public void handleCommandPacket(JSONObject json) throws JSONException,
			IOException {

		CommandPacket packet = new CommandPacket(json);
		LNSRequestInfo requestInfo = new LNSRequestInfo(packet.getRequestID(),
				packet);
		handler.addRequestInfo(packet.getRequestID(), requestInfo);
		packet = packet.removeSenderInfo();

		if (GnsProtocol.CREATE_DELETE_COMMANDS.contains(requestInfo
				.getCommandName())
				|| requestInfo.getCommandName().equals(GnsProtocol.SELECT))
			this.asyncLNSClient.sendRequestAnycast(packet, callback);
		else
			this.asyncLNSClient.sendRequest(packet, callback, redirector);
	}

  /**
   * Handles a command packet that has come in from a client.
   * 
   * @param json
   * @throws JSONException
   * @throws IOException
   */
  public void handleCommandPacketOld(JSONObject json) throws JSONException, IOException {

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
    	// arun
    	Util.suicide("Should never get here");
      actives = handler.getReplicatedActives(packet.getServiceName());
    }
    if (actives != null) {
      if (handler.isDebugMode()) {
        if (!disableRequestActives) {
          GNSConfig.getLogger().fine("Found actives in cache for " + packet.getServiceName() + ": " + actives);
        } else {
          GNSConfig.getLogger().fine("** USING DEFAULT ACTIVES for " + packet.getServiceName() + ": " + actives);
        }
      }
      if (disableCommandRetransmitter) {
        handler.sendToClosestReplica(actives, packet.toJSONObject());
      } else {
        handler.getProtocolExecutor().schedule(new CommandRetransmitter(requestId, packet.toJSONObject(),
                actives, handler));
      }

    }
    else {
      handler.getProtocolExecutor().schedule(new RequestActives(requestInfo, handler));
    }
  }

	/**
	 * Handles sending the results of a command packet back to the client.
	 * @param json
	 * @throws JSONException
	 * @throws IOException
	 */
	public void handleCommandReturnValuePacket(JSONObject json)
			throws JSONException, IOException {
		this.handleCommandReturnValuePacket(new CommandValueReturnPacket(json), json);
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
		CommandValueReturnPacket returnPacket = response instanceof CommandValueReturnPacket ? (CommandValueReturnPacket) response
				: null;
		ActiveReplicaError error = response instanceof ActiveReplicaError ? (ActiveReplicaError) response
				: null;
		GNSConfig.getLogger().log(
				Level.INFO,
				"{0} received response {1}",
				new Object[] {
						this,
						returnPacket != null ? returnPacket : error
								.getSummary() });
		assert (returnPacket != null || error != null);

		// FIXME: use long throughout
		long id = returnPacket != null ? returnPacket.getLNSRequestId()
				:  error.getRequestID();
		String serviceName = returnPacket != null ? returnPacket
				.getServiceName() : error.getServiceName();
		LNSRequestInfo sentInfo;
		GNSConfig.getLogger().log(Level.INFO, "{0} matching {1} with {2}",
				new Object[] { this, id+"", handler.getRequestInfo(id) });
		if ((sentInfo = handler.getRequestInfo(id)) != null) {
			// doublecheck that it is for the same service name
			if ((sentInfo.getServiceName()
					.equals(serviceName) ||
			// arun: except when service name is special name
			(sentInfo.getServiceName().equals(Config
					.getGlobalString(RC.SPECIAL_NAME))))) {
				// String serviceName = returnPacket.getServiceName();
				GNSConfig.getLogger().log(Level.INFO, "{0} about to remove {1}",
						new Object[] { this, id+"" });
				handler.removeRequestInfo(id);
				// update cache - if the service name isn't missing (invalid)
				// and if it is a READ command
				// FIXME: THIS ISN'T GOING TO WORK WITHOUT MORE INFO ABOUT THE
				// REQUEST
				if (!CommandPacket.BOGUS_SERVICE_NAME.equals(serviceName)
						&& sentInfo.getCommandType().equals(GnsProtocol.READ)
						&& returnPacket != null) {
					handler.updateCacheEntry(serviceName,
							returnPacket.getReturnValue());
				}
				// send the response back
				if (handler.isDebugMode()) 
				{
					GNSConfig.getLogger()
							.log(Level.INFO,
									"<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< LNS IS SENDING VALUE BACK TO {0}: {1}",
									new Object[] {
											sentInfo.getHost()+":"+
											sentInfo.getPort(),
											returnPacket != null ? returnPacket
													.getSummary() : error
													.getSummary() });
				}
				handler.sendToClient(new InetSocketAddress(sentInfo.getHost(),
						sentInfo.getPort()), json != null ? json
						: returnPacket != null ? returnPacket.toJSONObject()
								: error.toJSONObject());
			} else {
				GNSConfig.getLogger().severe(
						"Command response packet mismatch: "
								+ sentInfo.getServiceName() + " vs. "
								+ returnPacket.getServiceName());
			}
		} else {
			if (handler.isDebugMode()) 
			{
				GNSConfig.getLogger().info(
						"Duplicate response for "
								+ id
								+ ": "
								+ (json != null ? json : returnPacket!=null ? returnPacket
										.toJSONObject() : error.toJSONObject()));
			}
		}
	}
	
	public String toString() {
		return this.getClass().getSimpleName();
	}

  private void handleRequestActives(JSONObject json) {
    if (handler.isDebugMode()) {
      GNSConfig.getLogger().fine(")))))))))))))))))))))))))))) REQUEST ACTIVES RECEIVED: " + json.toString());

    }
    try {
      RequestActiveReplicas requestActives = new RequestActiveReplicas(json);
      if (requestActives.getActives() != null) {
        if (handler.isDebugMode()) {
          for (InetSocketAddress address : requestActives.getActives()) {
            GNSConfig.getLogger().fine("ACTIVE ADDRESS HOST: " + address.toString());
          }
        }
        // Update the cache so that request actives task will now complete
        handler.updateCacheEntry(requestActives.getServiceName(), requestActives.getActives());
        // also update the set of the nodes the ping manager is using
        handler.getPingManager().addActiveReplicas(requestActives.getActives());
      }
    } catch (JSONException e) {
      GNSConfig.getLogger().severe("Problem parsing RequestActiveReplicas packet info from " + json + ": " + e);
    }

  }

}
