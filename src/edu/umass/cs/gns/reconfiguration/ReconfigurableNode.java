package edu.umass.cs.gns.reconfiguration;

import java.io.IOException;

import edu.umass.cs.gns.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.gns.nio.JSONMessenger;
import edu.umass.cs.gns.nio.JSONNIOTransport;

/**
@author V. Arun
 */
public abstract class ReconfigurableNode<NodeIDType> {

	protected final NodeIDType myID; // only needed for app debugging in createAppCoordinator 
	protected abstract AbstractReplicaCoordinator<NodeIDType> createAppCoordinator();

	/*
	public ReconfigurableNode(NodeIDType id, InterfaceReconfigurableNodeConfig<NodeIDType> nc) throws IOException {
		this.myID = id;
		AbstractPacketDemultiplexer pd;
		JSONMessenger<NodeIDType> messenger = new JSONMessenger<NodeIDType>((new JSONNIOTransport<NodeIDType>(this.myID, 
				nc, (pd = new ReconfigurationPacketDemultiplexer()), true)).enableStampSenderInfo());
		ActiveReplica<NodeIDType> activeReplica = new ActiveReplica<NodeIDType>(createAppCoordinator(), nc, messenger);
		Reconfigurator<NodeIDType> reconfigurator = new Reconfigurator<NodeIDType>(nc, messenger);
		
		pd.register(activeReplica.getPacketTypes(), activeReplica); // includes app packets
		pd.register(reconfigurator.getPacketTypes().toArray(), reconfigurator);
		messenger.addPacketDemultiplexer(pd);
	}
	*/

	public ReconfigurableNode(NodeIDType id, InterfaceReconfigurableNodeConfig<NodeIDType> nc) throws IOException {
		this.myID = id;
		AbstractPacketDemultiplexer pd;

		if (!nc.getActiveReplicas().contains(id)
				&& !nc.getReconfigurators().contains(id))
			throw new RuntimeException("Node " + id
					+ " not present in NodeConfig argument " + nc.getActiveReplicas() + " " + nc.getReconfigurators());

		JSONMessenger<NodeIDType> messenger = new JSONMessenger<NodeIDType>((new JSONNIOTransport<NodeIDType>(this.myID, 
				nc, (pd = new ReconfigurationPacketDemultiplexer()), true)).enableStampSenderInfo());

		if(nc.getActiveReplicas().contains(id)) { // create active
			ActiveReplica<NodeIDType> activeReplica = new ActiveReplica<NodeIDType>(createAppCoordinator(), nc, messenger);
			pd.register(activeReplica.getPacketTypes(), activeReplica); // includes app packets
			messenger.addPacketDemultiplexer(pd);
		}
		if(nc.getReconfigurators().contains(id)) { // create reconfigurator
			Reconfigurator<NodeIDType> reconfigurator = new Reconfigurator<NodeIDType>(nc, messenger);
			pd.register(reconfigurator.getPacketTypes().toArray(), reconfigurator);
			messenger.addPacketDemultiplexer(pd);
		}
	}

}
