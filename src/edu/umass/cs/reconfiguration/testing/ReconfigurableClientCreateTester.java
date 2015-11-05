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
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
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
import edu.umass.cs.reconfiguration.reconfigurationutils.ConsistentReconfigurableNodeConfig;
import edu.umass.cs.utils.DelayProfiler;
import edu.umass.cs.utils.MyLogger;

/**
 * @author V. Arun
 * 
 *         This class is used to test batch creation of a large number of names.
 *         Individually creating names is rather slow, so batch creation helps
 *         significantly increase the creation throughput. For batch creation,
 *         all of the names in a batch must have the same set of initial active
 *         replicas.
 *         <p>
 * 
 *         Note: There is no corresponding batch deletion mechanism. In genral,
 *         different names will have different sets of active replicas, so it is
 *         not meaningful to delete a batch of names as an atomic operation.
 * 
 */

public class ReconfigurableClientCreateTester {

	private final Set<InetSocketAddress> reconfigurators;
	private final JSONMessenger<?> messenger;
	private final ConcurrentHashMap<String, Long> sentRequests = new ConcurrentHashMap<String, Long>();
	private final ConcurrentHashMap<String, BasicReconfigurationPacket<?>> rcvdResponses = new ConcurrentHashMap<String, BasicReconfigurationPacket<?>>();
	private Set<InetSocketAddress> activeReplicas = null;

	private Logger log = Logger.getLogger(getClass().getName());

	ReconfigurableClientCreateTester(Set<InetSocketAddress> reconfigurators,
			JSONMessenger<?> messenger) {
		this.reconfigurators = reconfigurators;
		this.messenger = messenger;
		messenger.addPacketDemultiplexer(new ClientPacketDemultiplexer());
	}

	/*
	 * This method makes a batched create request. The main piece of additional
	 * information needed here compared to a typical single create request is
	 * the set of reconfigurator node IDs as opposed to just their socket
	 * addresses. We need this because we need to ensure that all creates in a
	 * batch map to the same RC group. For this, we need RC IDs because IDs, not
	 * socket addresses, are used for consistent-hashing RCs on to the ring.
	 */
	private CreateServiceName[] makeCreateNameRequest(String name,
			String state, int batchSize) {
		Set<String> names = new HashSet<String>();
		for (int i = 0; i < batchSize; i++)
			names.add(name + i);
		Collection<Set<String>> batches = ConsistentReconfigurableNodeConfig
				.splitIntoRCGroups(names,
						ReconfigurationConfig.getReconfiguratorIDs());

		Set<CreateServiceName> creates = new HashSet<CreateServiceName>();
		// each batched create corresponds to a different RC group
		for (Set<String> batch : batches) {
			Map<String, String> nameStates = new HashMap<String, String>();
			for (String bname : batch) {
				nameStates.put(bname, state);
			}
			// a single batched create
			creates.add(new CreateServiceName(null, nameStates));
		}
		return creates.toArray(new CreateServiceName[0]);
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

	private InetSocketAddress getFirstRCReplica() {
		InetSocketAddress address = this.getReconfigurators().iterator().next();
		return new InetSocketAddress(address.getAddress(),
				ActiveReplica.getClientFacingPort(address.getPort()));
	}

	private static final boolean RANDOM_SERVER = true;

	private String sendRequest(BasicReconfigurationPacket<?> req)
			throws JSONException, IOException {
		InetSocketAddress sockAddr = (!RANDOM_SERVER ? this.getFirstRCReplica()
				: this.getRandomRCReplica());
		log.log(Level.INFO, MyLogger.FORMAT[7].replace(" ", ""), new Object[] {
				"Sending ", req.getSummary(), " to ", sockAddr, ":",
				(sockAddr)});
		this.sentRequests.put(req.getServiceName(), System.currentTimeMillis());
		this.sendRequest(sockAddr, req.toJSONObject());
		return req.getServiceName();
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
			long timeout) {
		while (sentRequests.containsKey(name)
		// || System.currentTimeMillis() - sentRequests.get(name) < timeout
		)
			try {
				wait(timeout);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		return rcvdResponses.remove(name);
	}

	synchronized BasicReconfigurationPacket<?> waitForReply(String name) {
		return this.waitForReply(name, REQUEST_TIMEOUT);
	}

	// only for ClientReconfigurationPacket
	synchronized boolean waitForSuccess(String name) {
		ClientReconfigurationPacket reply = (ClientReconfigurationPacket) this
				.waitForReply(name);
		System.out.println("unblocked from wait; reply = "
				+ (reply != null ? reply.getSummary() : "null"));
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
				RC_RECONFIGURE_TIMEOUT);
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
		try {
			/*
			 * Client can only send/receive clear text or do server-only
			 * authentication
			 */
			JSONMessenger<?> messenger = new JSONMessenger<String>(
					(new MessageNIOTransport<String, JSONObject>(null, null,
							new PacketDemultiplexerDefault(), true,
							ReconfigurationConfig.getClientSSLMode())));
			final ReconfigurableClientCreateTester client = new ReconfigurableClientCreateTester(
					ReconfigurationConfig.getReconfiguratorAddresses(),
					messenger);
			String initValue = "initVal";
			int numIterations = 1;

			for (int j = 0; j < numIterations; j++) {
				String namePrefix = "name"
						+ (int) (Math.random() * Integer.MAX_VALUE);
				long t0 = System.currentTimeMillis();

				// ///////////////// batched create name//////////////////////
				t0 = System.currentTimeMillis();
				// do
				int numCreates = 1;
				int batchSize = 50000;
				for (int i = 0; i < numCreates; i++) {
					final int k = i;
					try {
						// batch size is being specified here
						CreateServiceName[] creates = client
								.makeCreateNameRequest(namePrefix + k,
										initValue, batchSize);
						for (CreateServiceName create : creates) {
							client.sendRequest(create);
							System.out.println("Sent batched request of size "
									+ create.getNameStates().size());
							while (!client.waitForSuccess(create
									.getServiceName()))
								;
						}
					} catch (JSONException | IOException e) {
						e.printStackTrace();
					}
					System.out.println("SUCCESS " + k);
				}
				DelayProfiler.updateDelay("createName", t0);
				System.out.println(System.currentTimeMillis() - t0);
				System.exit(1);
			}

		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}
}
