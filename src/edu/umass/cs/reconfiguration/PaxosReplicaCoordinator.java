/*
 * Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 * 
 * Initial developer(s): V. Arun
 */
package edu.umass.cs.reconfiguration;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.umass.cs.gigapaxos.PaxosManager;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.paxosutil.PaxosInstanceCreationException;
import edu.umass.cs.gigapaxos.paxosutil.StringContainer;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.Messenger;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableRequest;
import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;
import edu.umass.cs.reconfiguration.reconfigurationpackets.BasicReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

/**
 * @author arun
 *
 * @param <NodeIDType>
 */
public class PaxosReplicaCoordinator<NodeIDType> extends
		AbstractReplicaCoordinator<NodeIDType> {

	private final PaxosManager<NodeIDType> paxosManager;
	protected static final Logger log = (Reconfigurator.getLogger());

	/**
	 * @param app
	 * @param myID
	 * @param unstringer
	 * @param niot
	 * @param enableNullCheckpoints
	 */
	@SuppressWarnings("unchecked")
	private PaxosReplicaCoordinator(Replicable app, NodeIDType myID,
			Stringifiable<NodeIDType> unstringer,
			Messenger<NodeIDType, ?> niot, String paxosLogFolder,
			boolean enableNullCheckpoints) {
		super(app, niot);
		assert (niot instanceof JSONMessenger);
		this.paxosManager = new PaxosManager<NodeIDType>(myID, unstringer,
				(JSONMessenger<NodeIDType>) niot, this, paxosLogFolder,
				enableNullCheckpoints);
	}

	protected void createDefaultGroup(String name, Set<String> strNodes,
			Stringifiable<NodeIDType> unstringer) {
		Set<NodeIDType> nodes = new HashSet<NodeIDType>();
		for (String strNode : strNodes)
			nodes.add(unstringer.valueOf(strNode));
		this.paxosManager.createPaxosInstance(name, nodes, null);
	}

	/**
	 * @param app
	 * @param myID
	 * @param unstringer
	 * @param niot
	 */
	public PaxosReplicaCoordinator(Replicable app, NodeIDType myID,
			Stringifiable<NodeIDType> unstringer,
			Messenger<NodeIDType, ?> niot) {
		this(app, myID, unstringer, niot, null, true);
	}

	/**
	 * @param app
	 * @param myID
	 * @param unstringer
	 * @param niot
	 * @param outOfOrderLimit
	 */
	@SuppressWarnings("unchecked")
	public PaxosReplicaCoordinator(Replicable app, NodeIDType myID,
			Stringifiable<NodeIDType> unstringer,
			Messenger<NodeIDType, ?> niot, int outOfOrderLimit) {
		this(app, myID, unstringer, (JSONMessenger<NodeIDType>) niot);
		assert (niot instanceof JSONMessenger);
		this.paxosManager.setOutOfOrderLimit(outOfOrderLimit);
	}

	@Override
	public Set<IntegerPacketType> getRequestTypes() {
		return this.app.getRequestTypes();
	}

	@Override
	public boolean coordinateRequest(Request request)
			throws IOException, RequestParseException {
		return this.coordinateRequest(request.getServiceName(), request);
	}

	private String propose(String paxosID, Request request) {
		String proposee = null;
		if (request instanceof ReconfigurableRequest
				&& ((ReconfigurableRequest) request).isStop())
			proposee = this.paxosManager
					.proposeStop(paxosID,
							((ReconfigurableRequest) request)
									.getEpochNumber(), request);
		else
			proposee = this.paxosManager.propose(paxosID, request);
		return proposee;
	}

	// in case paxosGroupID is not the same as the name in the request
	/**
	 * @param paxosGroupID
	 * @param request
	 * @return True if successfully proposed to some epoch of paxosGroupID.
	 * @throws RequestParseException
	 */
	public boolean coordinateRequest(String paxosGroupID,
			Request request) throws RequestParseException {
		String proposee = this.propose(paxosGroupID, request);
		log.log(Level.INFO,
				"{0} {1} request {2}:{3} [{4}] {5} to {6}",
				new Object[] {
						this,
						(proposee != null ? "paxos-coordinated"
								: "failed to paxos-coordinate"),
						request.getServiceName(),
						(request instanceof ReconfigurableRequest ? ((ReconfigurableRequest) request)
								.getEpochNumber() : "[]"),
						(request instanceof BasicReconfigurationPacket<?>) ? ((BasicReconfigurationPacket<?>) request)
								.getSummary() : request.getRequestType(),
						(request instanceof ReconfigurableRequest
								&& ((ReconfigurableRequest) request)
										.isStop() ? "[STOPPING]" : ""),
						proposee });
		return proposee != null;
	}

	/*
	 * This method always returns true as it will always succeed in either
	 * creating the group with the requested epoch number or higher. In either
	 * case, the caller should consider the operation a success.
	 */
	@Override
	public boolean createReplicaGroup(String groupName, int epoch,
			String state, Set<NodeIDType> nodes) {
		assert (state != null);
		// will block for a default timeout if a lower unstopped epoch exits
		boolean created = this.paxosManager.createPaxosInstanceForcibly(
				groupName, epoch, nodes, this, state, 0);
		boolean createdOrExistsOrHigher = (created || this.paxosManager
				.equalOrHigherVersionExists(groupName, epoch));
		;
		if (!createdOrExistsOrHigher)
			throw new PaxosInstanceCreationException((this
					+ " failed to create " + groupName + ":" + epoch
					+ " with state [" + state + "] likely because epoch "
					+ epoch + " or higher was previously created and stopped."));
		return createdOrExistsOrHigher;
	}
	
	@Override
	public boolean createReplicaGroup(Map<String, String> nameStates,
			Set<NodeIDType> nodes) {
		return this.paxosManager.createPaxosInstance(nameStates, nodes);
	}

	public String toString() {
		return this.getClass().getSimpleName() + getMyID();
	}

	@Override
	public Set<NodeIDType> getReplicaGroup(String serviceName) {
		/*
		 * if (this.paxosManager.isStopped(serviceName)) return null;
		 */
		return this.paxosManager.getReplicaGroup(serviceName);
	}

	@Override
	public boolean deleteReplicaGroup(String serviceName, int epoch) {
		return this.paxosManager.deleteStoppedPaxosInstance(serviceName, epoch);
	}

	protected void forceCheckpoint(String paxosID) {
		this.paxosManager.forceCheckpoint(paxosID);
	}

	@Override
	public Integer getEpoch(String name) {
		return this.paxosManager.getVersion(name);
	}

	@Override
	public String getFinalState(String name, int epoch) {
		StringContainer stateContainer = this.getFinalStateContainer(name,
				epoch);
		return stateContainer != null ? stateContainer.state : null;
	}

	/**
	 * Used by ActiveReplica and similar to getFinalState but wraps it in a
	 * container so that we can distinguish between null final state (a possibly
	 * legitimate value of the state) and no state at all (because the paxos
	 * group has moved on and deleted the state or never created it in the first
	 * place. An alternative is to disallow null as a legitimate app state, but
	 * that means forcing apps to specify a non-null initial state (currently
	 * not enforced) as initial state needs to be checkpointed for safety.
	 * 
	 * @param name
	 * @param epoch
	 * @return The final state wrapped in StringContainer.
	 */
	protected StringContainer getFinalStateContainer(String name, int epoch) {
		StringContainer stateContainer = this.paxosManager.getFinalState(name,
				epoch);
		String state = stateContainer != null ? stateContainer.state : null;
		log.log(Level.FINE,
				"{0} received request for epoch final state {1}:{2}; returning [{3}];)",
				new Object[] { this, name, epoch, state});
		return stateContainer;
	}

	/*
	 * It is a bad idea to use this method with paxos replica coordination. It
	 * is never a good idea to set paxos-maintained state through anything but
	 * paxos agreement, otherwise we may be violating safety. In the case of
	 * initial state, we (must) have agreement already on the value of the
	 * initial state, but we still need to have paxos initialize this state
	 * atomically with the creation of the paxos instance before any
	 * paxos-coordinated requests are executed.
	 */
	@Override
	public void putInitialState(String name, int epoch, String state) {
		throw new RuntimeException("This method should never have been called");
	}

	@Override
	public boolean deleteFinalState(String name, int epoch) {
		/*
		 * Will also delete one previous version. Sometimes, a node can miss a
		 * drop epoch that arrived even before it created that epoch, in which
		 * case, it would end up trying hard and succeeding at creating the
		 * epoch that just got dropped by using the previous epoch final state
		 * if it is available locally. So it is best to delete that final state
		 * as well so that the late, zombie epoch creation eventually fails.
		 * 
		 * Note: Usually deleting lower epochs in addition to the specified
		 * epoch is harmless. There is at most one lower epoch final state at a
		 * node anyway.
		 */
		return this.paxosManager.deleteFinalState(name, epoch);
	}

	@Override
	public ReconfigurableRequest getStopRequest(String name, int epoch) {
		ReconfigurableRequest stop = super.getStopRequest(name, epoch);
		if (stop != null && !(stop instanceof ReplicableRequest))
			throw new RuntimeException(
					"Stop requests for Paxos apps must implement InterfaceReplicableRequest "
							+ "and their needsCoordination() method must return true by default "
							+ "(unless overridden by setNeedsCoordination(false))");
		return stop;
	}

	/**
	 * @param node
	 * @return True if was being monitored.
	 */
	public boolean stopFailureMonitoring(NodeIDType node) {
		return this.paxosManager.stopFailureMonitoring(node);
	}

	/**
	 * @param name
	 * @param epoch
	 * @return True if the {@code epoch} or higher version exists for
	 *         {@code name}.
	 */
	public boolean existsOrHigher(String name, int epoch) {
		return this.paxosManager.equalOrHigherVersionExists(name, epoch);
	}
}
