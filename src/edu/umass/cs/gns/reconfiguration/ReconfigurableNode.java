package edu.umass.cs.gns.reconfiguration;

import java.io.IOException;

import edu.umass.cs.gns.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.gns.nio.JSONMessenger;
import edu.umass.cs.gns.nio.JSONNIOTransport;

/**
 * @author V. Arun
 */
public abstract class ReconfigurableNode<NodeIDType> {

	protected final NodeIDType myID;
	protected final InterfaceReconfigurableNodeConfig<NodeIDType> nodeConfig;
	protected final JSONMessenger<NodeIDType> messenger;

	protected abstract AbstractReplicaCoordinator<NodeIDType> createAppCoordinator();

	private ActiveReplica<NodeIDType> activeReplica;
	private Reconfigurator<NodeIDType> reconfigurator;
	
	public ReconfigurableNode(NodeIDType id,
			InterfaceReconfigurableNodeConfig<NodeIDType> nc)
			throws IOException {
		this.myID = id;
		this.nodeConfig = nc;

		AbstractPacketDemultiplexer pd;

		if (!nc.getActiveReplicas().contains(id)
				&& !nc.getReconfigurators().contains(id))
			throw new RuntimeException("Node " + id
					+ " not present in NodeConfig argument "
					+ nc.getActiveReplicas() + " " + nc.getReconfigurators());

		this.messenger = new JSONMessenger<NodeIDType>(
				(new JSONNIOTransport<NodeIDType>(this.myID, nc,
						(pd = new ReconfigurationPacketDemultiplexer()), true))
						.enableStampSenderInfo());

		if (nc.getActiveReplicas().contains(id)) {
			// create active
			ActiveReplica<NodeIDType> activeReplica = new ActiveReplica<NodeIDType>(
					createAppCoordinator(), nc, messenger);
			// getPacketTypes includes app's packets
			pd.register(activeReplica.getPacketTypes(), activeReplica);
			messenger.addPacketDemultiplexer(pd);
		} else if (nc.getReconfigurators().contains(id)) {
			// create reconfigurator
			Reconfigurator<NodeIDType> reconfigurator = new Reconfigurator<NodeIDType>(
					nc, messenger);
			pd.register(reconfigurator.getPacketTypes().toArray(),
					reconfigurator);
			messenger.addPacketDemultiplexer(pd);
			// wrap reconfigurator in ActiveReplica to make it reconfigurable
			//this.activeReplica = reconfigurator.getReconfigurableReconfiguratorAsActiveReplica();
		}
	}

	public void close() {
		if (this.activeReplica != null)
			this.activeReplica.close();
		if (this.reconfigurator != null)
			this.reconfigurator.close();
	}

}
