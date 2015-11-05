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
package edu.umass.cs.reconfiguration.reconfigurationutils;

import java.io.IOException;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONException;

import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.AbstractReplicaCoordinator;
import edu.umass.cs.reconfiguration.Reconfigurator;

/**
 * @author arun
 *
 * @param <NodeIDType>
 * 
 *            FIXME: Incomplete. This class is unusable.
 */
public class DynamoReplicaCoordinator<NodeIDType> extends
		AbstractReplicaCoordinator<NodeIDType> {

	private final ConsistentNodeConfig<NodeIDType> consistentNodeConfig;
	private static final Logger log = (Reconfigurator.getLogger());

	/**
	 * @param app
	 * @param myID
	 * @param nodeConfig
	 * @param messenger
	 */
	public DynamoReplicaCoordinator(Replicable app, NodeIDType myID,
			ConsistentNodeConfig<NodeIDType> nodeConfig,
			JSONMessenger<NodeIDType> messenger) {
		super(app, messenger);
		this.consistentNodeConfig = nodeConfig;
		throw new RuntimeException(
				"This class is incomplete and currently unusable. Check back later.");
	}

	@Override
	public Set<IntegerPacketType> getRequestTypes() {
		return this.app.getRequestTypes();
	}

	// FIXME: implement durability. Currently lazily propagates.
	@Override
	public boolean coordinateRequest(Request request)
			throws IOException, RequestParseException {
		try {
			log.log(Level.INFO, "{0} lazily coordinating {1}: {2}",
					new Object[] { this, request.getRequestType(), request });
			this.sendAllLazy(request);
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return true;
	}

	@Override
	public Set<NodeIDType> getReplicaGroup(String serviceName) {
		return this.consistentNodeConfig.getReplicatedServers(serviceName);
	}

	public String toString() {
		return this.getClass().getSimpleName() + getMyID();
	}

	/*
	 * Consistent hashing means that groups don't have to be explicitly created.
	 */
	@Override
	public boolean createReplicaGroup(String serviceName, int epoch,
			String state, Set<NodeIDType> nodes) {
		throw new RuntimeException(
				"This method should not be invoked as groups are "
				+ "implicitly defined with consistent hashing.");
	}

	@Override
	public boolean deleteReplicaGroup(String serviceName, int epoch) {
		throw new RuntimeException("Method not yet implemented");
	}
}
