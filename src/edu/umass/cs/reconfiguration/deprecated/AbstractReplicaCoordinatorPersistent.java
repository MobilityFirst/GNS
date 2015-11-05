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
package edu.umass.cs.reconfiguration.deprecated;

import java.util.Set;

import edu.umass.cs.gigapaxos.PaxosManager;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.nio.interfaces.Stringifiable;
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

	public AbstractReplicaCoordinatorPersistent(Replicable app,
			JSONMessenger<NodeIDType> messenger,
			Stringifiable<NodeIDType> unstringer) {
		super(app, messenger);
		this.paxosManager = new PaxosManager<NodeIDType>(messenger.getMyID(),
				unstringer, messenger, this, null);
	}

	// there must be no API to set paxosManager
	protected PaxosManager<NodeIDType> getPaxosManager() {
		return this.paxosManager;
	}

	@Override
	public boolean createReplicaGroup(String serviceName, int epoch,
			String state, Set<NodeIDType> nodes) {
		return this.paxosManager.createPaxosInstanceForcibly(serviceName,
				(short) epoch, nodes, this.app, state, 0);
	}

	@Override
	public boolean deleteReplicaGroup(String serviceName, int epoch) {
		return this.paxosManager.deleteStoppedPaxosInstance(serviceName, (short) epoch);
	}

	@Override
	public Set<NodeIDType> getReplicaGroup(String serviceName) {
		return this.paxosManager.getReplicaGroup(serviceName);
	}
}
