/*
 * Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * Initial developer(s): V. Arun
 */
package edu.umass.cs.reconfiguration.testing;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.nio.AbstractJSONPacketDemultiplexer;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.nioutils.PacketDemultiplexerDefault;
import edu.umass.cs.nio.nioutils.StringifiableDefault;
import edu.umass.cs.reconfiguration.AbstractReconfiguratorDB;
import edu.umass.cs.reconfiguration.ActiveReplica;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.reconfiguration.examples.AppRequest;
import edu.umass.cs.reconfiguration.reconfigurationpackets.BasicReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ClientReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.CreateServiceName;
import edu.umass.cs.reconfiguration.reconfigurationpackets.DeleteServiceName;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigureRCNodeConfig;
import edu.umass.cs.reconfiguration.reconfigurationpackets.RequestActiveReplicas;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.utils.DelayProfiler;
import edu.umass.cs.utils.MyLogger;

/**
 * @author V. Arun
 * 
 *         FIXME: This is an example client for testing purposes. A cleaner
 *         "AbstractReconfigurationClient" is TBD.
 * 
 *         This class starts with a set of reconfigurator socket addresses known
 *         a priori and shows the following operations for a name (1) request
 *         active replicas, (2) creation, and (3) deletion. It also shows how to
 *         add and remove reconfigurator nodes, but these latter operations are
 *         normally for use only by administrators, not clients.
 */
public class ReconfigurableClient {

	private final Set<InetSocketAddress> reconfigurators;
	private final JSONMessenger<?> messenger;
	private final ConcurrentHashMap<String, Long> sentRequests = new ConcurrentHashMap<String, Long>();
	private final ConcurrentHashMap<String, BasicReconfigurationPacket<?>> rcvdResponses = new ConcurrentHashMap<String, BasicReconfigurationPacket<?>>();
	private Set<InetSocketAddress> activeReplicas = null;

	private Logger log = Logger.getLogger(getClass().getName());

	ReconfigurableClient(Set<InetSocketAddress> reconfigurators,
			JSONMessenger<?> messenger) {
		this.reconfigurators = reconfigurators;
		this.messenger = messenger;
		messenger.addPacketDemultiplexer(new ClientPacketDemultiplexer());
	}

	private AppRequest makeRequest(String name, String value) {
		return new AppRequest(name, value,
				AppRequest.PacketType.DEFAULT_APP_REQUEST, false);
	}

	private CreateServiceName makeCreateNameRequest(String name, String state) {
		return new CreateServiceName(null, name, 0, state);
	}

	private DeleteServiceName makeDeleteNameRequest(String name) {
		return new DeleteServiceName(null, name, 0);
	}

	private RequestActiveReplicas makeRequestActiveReplicas(String name) {
		return new RequestActiveReplicas(null, name, 0);
	}

	// active replicas should not be hard-coded
	private InetSocketAddress getRandomActiveReplica() {
		int index = (int) (this.getActiveReplicas().size() * Math.random());
		return (InetSocketAddress) (this.getActiveReplicas().toArray()[index]);

	}

	private Set<InetSocketAddress> getReconfigurators() {
		return this.reconfigurators;
	}

	private InetSocketAddress getRandomRCReplica() {
		int index = (int) (this.getReconfigurators().size() * Math.random());
		InetSocketAddress address = (InetSocketAddress) (this
				.getReconfigurators().toArray()[index]);
		return new InetSocketAddress(address.getAddress(),
				ActiveReplica.getClientFacingPort(address.getPort()));
	}

	private InetSocketAddress getFirstActiveReplica() {
		return this.getActiveReplicas().iterator().next();
	}

	private InetSocketAddress getFirstRCReplica() {
		InetSocketAddress address = this.getReconfigurators().iterator().next();
		return new InetSocketAddress(address.getAddress(),
				ActiveReplica.getClientFacingPort(address.getPort()));
	}

	private static final boolean RANDOM_SERVER = true;

	private void sendRequest(AppRequest req) throws JSONException, IOException,
			RequestParseException {
		InetSocketAddress sockAddr = (!RANDOM_SERVER ? this
				.getFirstActiveReplica() : this.getRandomActiveReplica());
		log.log(Level.INFO, MyLogger.FORMAT[7].replace(" ", ""), new Object[] {
				"Sending ", req.getRequestType(), " to ", sockAddr, ":",
				(sockAddr), ": ", req });
		this.sentRequests.put(req.getServiceName(), System.currentTimeMillis());
		this.sendRequest(sockAddr, req.toJSONObject());
	}

	private void sendRequest(BasicReconfigurationPacket<?> req)
			throws JSONException, IOException {
		InetSocketAddress sockAddr = (!RANDOM_SERVER ? this.getFirstRCReplica()
				: this.getRandomRCReplica());
		log.log(Level.INFO, MyLogger.FORMAT[7].replace(" ", ""), new Object[] {
				"Sending ", req.getSummary(), " to ", sockAddr, ":",
				(sockAddr), ": ", req });
		this.sentRequests.put(req.getServiceName(), System.currentTimeMillis());
		this.sendRequest(sockAddr, req.toJSONObject());
	}

	private void sendRequest(InetSocketAddress id, JSONObject json)
			throws JSONException, IOException {
		// modify
		this.messenger.sendToAddress(id, json);
	}

	private class ClientPacketDemultiplexer extends
			AbstractJSONPacketDemultiplexer {

		ClientPacketDemultiplexer() {
			this.register(ReconfigurationPacket.PacketType.CREATE_SERVICE_NAME);
			this.register(ReconfigurationPacket.PacketType.DELETE_SERVICE_NAME);
			this.register(AppRequest.PacketType.DEFAULT_APP_REQUEST);
			this.register(ReconfigurationPacket.PacketType.REQUEST_ACTIVE_REPLICAS);
			this.register(ReconfigurationPacket.PacketType.RECONFIGURE_RC_NODE_CONFIG);
		}

		@Override
		public boolean handleMessage(JSONObject json) {
			log.log(Level.FINEST, "Client received {0}", new Object[] { json });
			try {
				ReconfigurationPacket.PacketType rcType = ReconfigurationPacket
						.getReconfigurationPacketType(json);
				if (rcType != null) {
					switch (ReconfigurationPacket
							.getReconfigurationPacketType(json)) {
					case CREATE_SERVICE_NAME:
						CreateServiceName create = new CreateServiceName(json);
						log.log(Level.INFO,
								"Received create {0} for name {1} {2}",
								new Object[] {
										(create.isFailed() ? " ***********ERROR*********** "
												: " CONFIRMATION "),
										create.getServiceName(),
										create.isFailed() ? ": "
												+ create.getResponseMessage()
												: "" });
						notifyReply(create);
						break;

					case DELETE_SERVICE_NAME:
						DeleteServiceName delete = new DeleteServiceName(json);
						log.log(Level.INFO,
								"Received delete {0} for name {1} {2}",
								new Object[] {
										delete.isFailed() ? " ERROR "
												: " CONFIRMATION ",
										delete.getServiceName(),
										delete.isFailed() ? ": "
												+ delete.getResponseMessage()
												: "" });
						notifyReply(delete);
						break;
					case REQUEST_ACTIVE_REPLICAS:
						RequestActiveReplicas reqActives = new RequestActiveReplicas(
								json);
						log.log(Level.INFO,
								"Received active replicas for {0} : {1}",
								new Object[] { reqActives.getServiceName(),
										reqActives.getActives() });
						activeReplicas = reqActives.getActives();
						// we want to put failed responses here in rcvsResponses
						notifyAnyReply(reqActives);
						break;
					case RECONFIGURE_RC_NODE_CONFIG:
						ReconfigureRCNodeConfig<String> rcnc = new ReconfigureRCNodeConfig<String>(
								json, new StringifiableDefault<String>(""));
						log.log(Level.INFO,
								"Received node config change {0} {1}{2}",
								new Object[] {
										rcnc.isFailed() ? "ERROR: "
												+ rcnc.getMessage()
												: "confirmation",
										(rcnc.newlyAddedNodes != null ? "; added"
												+ rcnc.newlyAddedNodes
												: ""),
										(rcnc.deletedNodes != null ? "; deleted"
												+ rcnc.deletedNodes
												: "") });
						notifyReply(rcnc);
						;
						break;

					default:
						break;
					}
				}

				AppRequest.PacketType type = AppRequest.PacketType
						.getPacketType(JSONPacket.getPacketType(json));
				if (type != null) {
					switch (AppRequest.PacketType.getPacketType(JSONPacket
							.getPacketType(json))) {
					case DEFAULT_APP_REQUEST:
						AppRequest request = new AppRequest(json);
						log.log(Level.INFO,
								MyLogger.FORMAT[1],
								new Object[] {
										"App executed request",
										request.getRequestID() + ":"
												+ request.getValue() });
						sentRequests.remove(request.getServiceName());
						notifyReply();
						break;
					case ANOTHER_APP_REQUEST:
						throw new RuntimeException(
								"Client received unexpected APP_COORDINATION message");
					}
				}

			} catch (JSONException je) {
				je.printStackTrace();
			} catch (RuntimeException re) {
				re.printStackTrace();
				fatalException(re);
			}
			return true;
		}
	}

	private void fatalException(Exception e) {
		System.out.println("!!!!!FATAL exception: " + e.getMessage()
				+ "; exiting!!!!!!");
		System.exit(1);

	}

	Set<InetSocketAddress> getActiveReplicas() {
		return this.activeReplicas;
	}

	private static final long REQUEST_TIMEOUT = 2000;
	private static final long RC_RECONFIGURE_TIMEOUT = 4000;

	synchronized BasicReconfigurationPacket<?> waitForReply(String name,
			long timeout, boolean retransmission) {
		while (sentRequests.containsKey(name)
				&& (!retransmission || (System.currentTimeMillis()
						- sentRequests.get(name) < timeout)))
			try {
				wait(timeout);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		return rcvdResponses.remove(name);
	}

	synchronized BasicReconfigurationPacket<?> waitForReply(String name) {
		return this.waitForReply(name, REQUEST_TIMEOUT, true);
	}

	// only for ClientReconfigurationPacket
	synchronized boolean waitForSuccess(String name) {
		ClientReconfigurationPacket reply = (ClientReconfigurationPacket) this
				.waitForReply(name);
		return reply != null && !reply.isFailed();
	}

	// only for ClientReconfigurationPacket
	synchronized boolean waitForFailure(String name) {
		ClientReconfigurationPacket reply = (ClientReconfigurationPacket) this
				.waitForReply(name);
		return reply != null && reply.isFailed();
	}

	synchronized boolean waitForReconfigureRCSuccess(String name) {
		BasicReconfigurationPacket<?> reply = this.waitForReply(name,
				RC_RECONFIGURE_TIMEOUT, false);
		return reply != null && reply instanceof ReconfigureRCNodeConfig<?>
				&& !((ReconfigureRCNodeConfig<?>) reply).isFailed();
	}

	private static final long APP_REQUEST_TIMEOUT = 200;

	synchronized boolean rcvdAppReply(String name) {
		if (sentRequests.containsKey(name))
			try {
				wait(APP_REQUEST_TIMEOUT);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		return !sentRequests.containsKey(name);
	}

	synchronized void notifyReply(ClientReconfigurationPacket response) {
		if (!response.isFailed()
				&& sentRequests.remove(response.getServiceName()) != null)
			rcvdResponses.put(response.getServiceName(), response);
		notify();
	}

	synchronized void notifyAnyReply(ClientReconfigurationPacket response) {
		if (sentRequests.remove(response.getServiceName()) != null)
			rcvdResponses.put(response.getServiceName(), response);
		notify();

	}

	synchronized void notifyReply() {
		notify();
	}

	synchronized void notifyReply(ReconfigureRCNodeConfig<?> response) {
		if (!response.isFailed()
				&& sentRequests.remove(response.getServiceName()) != null)
			rcvdResponses.put(response.getServiceName(), response);
		notify();
	}

	/**
	 * 
	 */
	public static int TEST_PORT = 61000;

	/**
	 * Simple test client for the reconfiguration package. Clients only know the
	 * set of all reconfigurators, not active replicas for any name. All
	 * information about active replicas for a name is obtained from
	 * reconfigurators. Any request can be sent to any reconfigurator and it
	 * will forward to the appropriate reconfigurator if necessary and relay
	 * back the response.
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		ReconfigurableClient client = null;
		try {
			/*
			 * Client can only send/receive clear text or do server-only
			 * authentication
			 */
			JSONMessenger<?> messenger = new JSONMessenger<String>(
					(new MessageNIOTransport<String, JSONObject>(null, null,
							new PacketDemultiplexerDefault(), true,
							ReconfigurationConfig.getClientSSLMode())));
			client = new ReconfigurableClient(
					ReconfigurationConfig.getReconfiguratorAddresses(),
					messenger);
			int numRequests = 2;
			String requestValuePrefix = "request_value";
			long nameReqInterArrivalTime = 200;
			long NCReqInterArrivalTime = 1000;
			String initValue = "initial_value";
			int numIterations = 10000;
			boolean testReconfigureRC = true;

			for (int j = 0; j < numIterations; j++) {
				String namePrefix = "name"
						+ (int) (Math.random() * Integer.MAX_VALUE);
				String reconfiguratorID = "RC" + (int) (Math.random() * 64000);
				long t0 = System.currentTimeMillis();

				// /////////////request active replicas////////////////////
				t0 = System.currentTimeMillis();
				do
					client.sendRequest(client
							.makeRequestActiveReplicas(namePrefix));
				while (!client.waitForFailure(namePrefix));
				DelayProfiler.updateDelay("requestActives", t0);

				// active replicas for name initially don't exist
				assert (client.getActiveReplicas() == null || client
						.getActiveReplicas().isEmpty());
				// ////////////////////////////////////////////////////////

				// ////////////////////create name/////////////////////////
				t0 = System.currentTimeMillis();
				do
					client.sendRequest(client.makeCreateNameRequest(namePrefix,
							initValue));
				while (!client.waitForSuccess(namePrefix));
				DelayProfiler.updateDelay("createName", t0);
				// ////////////////////////////////////////////////////////

				/*
				 * Verify that active replicas for name now exist. The only
				 * reason the query is repeated is because it is possible to
				 * find the name non-existent briefly if the query is sent to a
				 * different reconfigurator that hasn't yet caught up with the
				 * creation (but will eventually do so).
				 */
				// ////////////////////////////////////////////////////////
				t0 = System.currentTimeMillis();
				do
					client.sendRequest(client
							.makeRequestActiveReplicas(namePrefix));
				while (!client.waitForSuccess(namePrefix));
				DelayProfiler.updateDelay("requestActives", t0);

				assert (client.getActiveReplicas() != null && !client
						.getActiveReplicas().isEmpty());
				// ////////////////////////////////////////////////////////

				// ///////send a stream of app requests sequentially///////
				for (int i = 0; i < numRequests; i++) {
					t0 = System.currentTimeMillis();
					do
						client.sendRequest(client.makeRequest(namePrefix,
								requestValuePrefix + i));
					while (!client.rcvdAppReply(namePrefix));
					DelayProfiler.updateDelay("appPaxosRequest", t0);
					Thread.sleep(nameReqInterArrivalTime);
				}
				// ////////////////////////////////////////////////////////

				// ////////////////////////////////////////////////////////
				// request current active replicas (possibly reconfigured)
				t0 = System.currentTimeMillis();
				do
					client.sendRequest(client
							.makeRequestActiveReplicas(namePrefix));
				while (!client.waitForSuccess(namePrefix));
				DelayProfiler.updateDelay("requestActives", t0);
				// ////////////////////////////////////////////////////////

				// ///////////////delete name, retransmit if error////////////
				t0 = System.currentTimeMillis();
				do
					client.sendRequest(client.makeDeleteNameRequest(namePrefix));
				while (!client.waitForSuccess(namePrefix));
				DelayProfiler.updateDelay("deleteName", t0);

				Thread.sleep(nameReqInterArrivalTime);
				// ////////////////////////////////////////////////////////

				// ////////////////////////////////////////////////////////
				// verify that active replicas for name now don't exist. The
				t0 = System.currentTimeMillis();
				do
					client.sendRequest(client
							.makeRequestActiveReplicas(namePrefix));
				while (!client.waitForFailure(namePrefix));
				DelayProfiler.updateDelay("requestActives", t0);

				assert (client.getActiveReplicas() == null || client
						.getActiveReplicas().isEmpty());
				// ////////////////////////////////////////////////////////

				if (!testReconfigureRC)
					continue;

				// ////////////////////////////////////////////////////////
				// add RC node; the port below does not matter in this test
				t0 = System.currentTimeMillis();
				// do
				client.sendRequest(new ReconfigureRCNodeConfig<String>(null,
						reconfiguratorID, new InetSocketAddress(InetAddress
								.getByName("localhost"), TEST_PORT)));
				while (!client
						.waitForReconfigureRCSuccess(AbstractReconfiguratorDB.RecordNames.NODE_CONFIG
								.toString()))
					;
				DelayProfiler.updateDelay("addReconfigurator", t0);
				// ////////////////////////////////////////////////////////

				Thread.sleep(NCReqInterArrivalTime);

				// //////////////// delete just added RC node//////////////////
				HashSet<String> deleted = new HashSet<String>();
				deleted.add(reconfiguratorID);
				t0 = System.currentTimeMillis();
				// do
				client.sendRequest(new ReconfigureRCNodeConfig<String>(null,
						null, deleted));
				while (!client
						.waitForReconfigureRCSuccess(AbstractReconfiguratorDB.RecordNames.NODE_CONFIG
								.toString())) {
				}
				DelayProfiler.updateDelay("removeReconfigurator", t0);
				// ////////////////////////////////////////////////////////

				Thread.sleep(NCReqInterArrivalTime);

				client.log
						.info("\n\n\n\n==================Successfully completed iteration "
								+ j
								+ ":\n"
								+ DelayProfiler.getStats()
								+ "\n\n\n\n");
			}

			// client.messenger.stop();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		} catch (JSONException je) {
			je.printStackTrace();
		} catch (InterruptedException ie) {
			ie.printStackTrace();
		} catch (RequestParseException e) {
			e.printStackTrace();
		}
	}
}
