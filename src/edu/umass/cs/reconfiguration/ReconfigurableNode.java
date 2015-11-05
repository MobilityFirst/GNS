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
package edu.umass.cs.reconfiguration;

import java.io.IOException;

import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gigapaxos.interfaces.ClientMessenger;
import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.nio.AbstractJSONPacketDemultiplexer;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.nio.JSONNIOTransport;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableNodeConfig;
import edu.umass.cs.reconfiguration.reconfigurationutils.DefaultNodeConfig;
import edu.umass.cs.reconfiguration.reconfigurationutils.ReconfigurationPacketDemultiplexer;

/**
 * 
 * @author V. Arun
 *
 * @param <NodeIDType>
 *            A generic type for representing node identifiers. It must support
 *            an explicitly overridden toString() method that converts
 *            NodeIDType to String, and the NodeConfig object supplied to this
 *            class' constructor must support a valueOf(String) method that
 *            returns back the original NodeIDType. Thus, even though NodeIDType
 *            is generic, a one-to-one mapping between NodeIDType and String is
 *            necessary.
 */
public abstract class ReconfigurableNode<NodeIDType> {

	protected final NodeIDType myID;
	protected final ReconfigurableNodeConfig<NodeIDType> nodeConfig;
	protected final JSONMessenger<NodeIDType> messenger;

	protected abstract AbstractReplicaCoordinator<NodeIDType> createAppCoordinator();

	private ActiveReplica<NodeIDType> activeReplica;
	private Reconfigurator<NodeIDType> reconfigurator;

	/**
	 * @param id
	 * @param nodeConfig
	 * @throws IOException
	 */
	public ReconfigurableNode(NodeIDType id,
			ReconfigurableNodeConfig<NodeIDType> nodeConfig)
			throws IOException {
		this(id, nodeConfig, false);
	}

	/**
	 * Close gracefully.
	 */
	public void close() {
		if (this.activeReplica != null)
			this.activeReplica.close();
		if (this.reconfigurator != null)
			this.reconfigurator.close();
	}

	private AbstractReplicaCoordinator<NodeIDType> createApp() {
		if (ReconfigurationConfig.application != null) {
			Replicable app = ReconfigurationConfig.createApp();
			if (app instanceof ClientMessenger)
				((ClientMessenger) app).setClientMessenger(messenger);
			else
				Reconfigurator
						.getLogger()
						.info(app.getClass().getSimpleName()
								+ " does not implement "
								+ ClientMessenger.class
										.getSimpleName()
								+ ", which means the app should either rely on "
								+ ClientRequest.class.getSimpleName()
								+ " or not expect to send "
								+ " responses back to clients or rely on alternate means for messaging.");
			PaxosReplicaCoordinator<NodeIDType> prc = new PaxosReplicaCoordinator<NodeIDType>(app, myID,
					nodeConfig, messenger);
			prc.createDefaultGroup(app.getClass().getSimpleName() + "0",
					PaxosConfig.getActives().keySet(), nodeConfig);
			return prc;
		} else
			return this.createAppCoordinator();
	}

	/**
	 * @param id
	 *            Node ID of this ReconfigurableNode being created.
	 * @param nodeConfig
	 *            Maps node IDs of active replicas and reconfigurators to their
	 *            socket addresses.
	 * @param startCleanSlate
	 *            Used to join newly added nodes.
	 * 
	 * @throws IOException
	 *             Thrown if networking functions can not be successfully
	 *             initialized. A common reason for this exception is that the
	 *             socket addresses corresponding to the supplied 'id' argument
	 *             are not local, i.e., the node with this id should not be
	 *             created on this machine in the first place, or if the id is
	 *             not present at all in the supplied 'nodeConfig' argument.
	 */
	public ReconfigurableNode(NodeIDType id,
			ReconfigurableNodeConfig<NodeIDType> nodeConfig,
			boolean startCleanSlate) throws IOException {
		this.myID = id;
		this.nodeConfig = nodeConfig;

		AbstractJSONPacketDemultiplexer pd;

		if (!nodeConfig.getActiveReplicas().contains(id)
				&& !nodeConfig.getReconfigurators().contains(id))
			Reconfigurator.getLogger().severe(
					"Node " + id + " not present in NodeConfig argument \n  "
							+ nodeConfig.getActiveReplicas() + "\n  "
							+ nodeConfig.getReconfigurators());
		// else we have something to start
		messenger = (new JSONMessenger<NodeIDType>(
				(new JSONNIOTransport<NodeIDType>(ReconfigurableNode.this.myID,
						nodeConfig,
						(pd = new ReconfigurationPacketDemultiplexer()),
						ReconfigurationConfig.getServerSSLMode()))));

		if (nodeConfig.getActiveReplicas().contains(id)) {
			// create active
			ActiveReplica<NodeIDType> activeReplica = new ActiveReplica<NodeIDType>(
			// createAppCoordinator(),
					createApp(), nodeConfig, messenger);
			// getPacketTypes includes app's packets
			pd.register(activeReplica.getPacketTypes(), activeReplica);
		} else if (nodeConfig.getReconfigurators().contains(id)) {
			// create reconfigurator
			Reconfigurator<NodeIDType> reconfigurator = new Reconfigurator<NodeIDType>(
					nodeConfig, messenger, startCleanSlate);
			pd.register(reconfigurator.getPacketTypes().toArray(new IntegerPacketType[0]),
					reconfigurator);

			// wrap reconfigurator in active to make it reconfigurable
			ReconfigurableNode.this.activeReplica = reconfigurator
					.getReconfigurableReconfiguratorAsActiveReplica();
			pd.register(activeReplica.getPacketTypes(),
					ReconfigurableNode.this.activeReplica);
		}
	}


	// because ReconfigurableNode is abstract for backwards compatibility
	static class DefaultReconfigurableNode extends ReconfigurableNode<String> {

		public DefaultReconfigurableNode(String id,
				ReconfigurableNodeConfig<String> nodeConfig,
				boolean startCleanSlate) throws IOException {
			super(id, nodeConfig, startCleanSlate);
		}

		@Override
		protected AbstractReplicaCoordinator<String> createAppCoordinator() {
			return super.createApp();
		}
	}

	/**
	 * {@code args} only contains the list of node IDs that we should try to
	 * start in this JVM. In a typical distributed setting, we only expect one
	 * node ID as an argument, or two if we are co-locating actives and
	 * reconfigurators.
	 * 
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		if (args.length == 0)
			throw new RuntimeException(
					"At least one node ID must be specified as a command-line argument for starting "
							+ ReconfigurableNode.class);
		ReconfigurationConfig.setConsoleHandler();
		ReconfigurableNodeConfig<String> nodeConfig = new DefaultNodeConfig<String>(
				PaxosConfig.getActives(),
				ReconfigurationConfig.getReconfigurators());
		System.out.print("Creating node(s) [ ");
		for (int i = args.length - 1; i >= 0 && nodeConfig.nodeExists(args[i]); i--)
			if (nodeConfig.nodeExists(args[i])) {
				System.out.print(args[i] + ":"
						+ nodeConfig.getNodeAddress(args[i]) + ":"
						+ nodeConfig.getNodePort(args[i]) + " ");
				new DefaultReconfigurableNode(args[i], 
						// must use a different nodeConfig for each
						new DefaultNodeConfig<String>(
						PaxosConfig.getActives(),
						ReconfigurationConfig.getReconfigurators()), false);
			}
		System.out.println("]");
	}
}
