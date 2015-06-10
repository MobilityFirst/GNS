package edu.umass.cs.reconfiguration.deprecated;

import java.util.Set;

import edu.umass.cs.gigapaxos.InterfaceReplicable;
import edu.umass.cs.gigapaxos.PaxosManager;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.nio.Stringifiable;
import edu.umass.cs.reconfiguration.AbstractReplicaCoordinator;

/**
 * @author arun
 *
 * @param <NodeIDType>
 * 
 *            FIXME: a hack(?) to use paxos just for persistent record keeping
 *            of epoch related info at a replica coordinator. What is really
 *            needed here is just a DB, but paxos provides similar features, so
 *            it is handy to use even when the consensus part of it is not
 *            really needed. When the concrete replica coordinator does need
 *            paxos, it can conveniently just use this paxos manager.
 * 
 *            This class is incomplete and may get deprecated.
 */
@SuppressWarnings("javadoc")
public abstract class AbstractReplicaCoordinatorPersistent<NodeIDType> extends
		AbstractReplicaCoordinator<NodeIDType> {

	private final PaxosManager<NodeIDType> paxosManager;

	public AbstractReplicaCoordinatorPersistent(InterfaceReplicable app,
			JSONMessenger<NodeIDType> messenger,
			Stringifiable<NodeIDType> unstringer) {
		super(app, messenger);
		this.paxosManager = new PaxosManager<NodeIDType>(messenger.getMyID(),
				unstringer, messenger, this);
	}

	// there must be no API to set paxosManager
	protected PaxosManager<NodeIDType> getPaxosManager() {
		return this.paxosManager;
	}

	@Override
	public boolean createReplicaGroup(String serviceName, int epoch,
			String state, Set<NodeIDType> nodes) {
		return this.paxosManager.createPaxosInstanceForcibly(serviceName,
				(short) epoch, nodes, this.app, state);
	}

	@Override
	public void deleteReplicaGroup(String serviceName, int epoch) {
		this.paxosManager.deletePaxosInstance(serviceName, (short) epoch);
	}

	@Override
	public Set<NodeIDType> getReplicaGroup(String serviceName) {
		return this.paxosManager.getPaxosNodeIDs(serviceName);
	}

	public boolean isActive(String serviceName, int epoch) {
		return this.paxosManager.isActive(serviceName, (short) epoch);
	}
}
