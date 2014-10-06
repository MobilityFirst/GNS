package edu.umass.cs.gns.reconfiguration;

import java.io.IOException;

import edu.umass.cs.gns.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.gns.nio.JSONMessenger;
import edu.umass.cs.gns.nio.JSONNIOTransport;
import edu.umass.cs.gns.nio.nioutils.PacketDemultiplexerDefault;

/**
@author V. Arun
 */
public abstract class ReconfigurableNode<NodeIDType> {

	protected final NodeIDType myID; // only needed for app debugging in createAppCoordinator 
	protected final ActiveReplica<NodeIDType> activeReplica;
	protected final Reconfigurator<NodeIDType> reconfigurator;

	protected abstract AbstractReplicaCoordinator<NodeIDType> createAppCoordinator();

	public ReconfigurableNode(NodeIDType id, InterfaceReconfigurableNodeConfig<NodeIDType> nc) throws IOException {
		this.myID = id;
		AbstractPacketDemultiplexer pd;
		JSONMessenger<NodeIDType> messenger = new JSONMessenger<NodeIDType>((new JSONNIOTransport<NodeIDType>(this.myID, 
				nc, (pd = new PacketDemultiplexerDefault()), true)).enableStampSenderInfo());
		this.activeReplica = new ActiveReplica<NodeIDType>(createAppCoordinator(), nc, messenger);
		this.reconfigurator = new Reconfigurator<NodeIDType>(nc, messenger);
		
		pd.register(this.activeReplica.getPacketTypes(), this.activeReplica); // includes app packets
		pd.register(this.reconfigurator.getPacketTypes().toArray(), this.reconfigurator);
		messenger.addPacketDemultiplexer(pd);
	}
}
