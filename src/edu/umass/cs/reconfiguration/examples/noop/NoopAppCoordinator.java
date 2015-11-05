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
package edu.umass.cs.reconfiguration.examples.noop;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.SSLMessenger;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.reconfiguration.PaxosReplicaCoordinator;
import edu.umass.cs.reconfiguration.interfaces.Reconfigurable;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableRequest;
import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

/**
 * @author V. Arun
 * 
 *         An explicit coordinator class like this is optional and is needed
 *         only of the application wants to implement its own replica
 *         coordination scheme. For applications simply wanting to use gigapaxos
 *         as their replica coordination protocol, a class like this is
 *         unnecessary. The code below shows how to use a replica coordination
 *         scheme based on lazy propagation as an alternative to paxos. Note
 *         that the lazy propagation scheme below is not even eventually
 *         consistent and is shown here only as a trivial alternative example.
 */
public class NoopAppCoordinator extends PaxosReplicaCoordinator<String> {

	/**
	 * Intended to support two types of coordination policies: lazy and paxos.
	 */
	public static enum CoordType {
		/**
		 * Lazily propagates coordination-needing requests to all replicas.
		 */
		LAZY,
		/**
		 * Coordinates coordination-needing requests using gigapaxos.
		 */
		PAXOS
	};

	private final CoordType coordType;

	private class CoordData {
		final String name;
		final int epoch;
		final Set<String> replicas;

		CoordData(String name, int epoch, Set<String> replicas) {
			this.name = name;
			this.epoch = epoch;
			this.replicas = replicas;
		}
	}

	private final HashMap<String, CoordData> groups = new HashMap<String, CoordData>();

	NoopAppCoordinator(Replicable app) {
		this(app, CoordType.PAXOS, null, null);
	}

	NoopAppCoordinator(Replicable app, CoordType coordType,
			Stringifiable<String> unstringer,
			SSLMessenger<String, JSONObject> msgr) {
		super(app, msgr.getMyID(), unstringer, msgr);
		this.coordType = coordType;
		this.registerCoordination(NoopAppRequest.PacketType.DEFAULT_APP_REQUEST);
		if (app instanceof NoopApp)
			((NoopApp) app).setClientMessenger(msgr);
	}

	@Override
	public boolean coordinateRequest(Request request)
			throws IOException, RequestParseException {
		try {
			// coordinate exactly once, and set self to entry replica
			if (request instanceof ReplicableRequest)
				((ReplicableRequest) request)
						.setNeedsCoordination(false);
			if (request instanceof NoopAppRequest)
				((NoopAppRequest) request).setEntryReplica(this.getMyID());
			// pick lazy or paxos coordinator, the defaults supported
			if (this.coordType.equals(CoordType.LAZY))
				this.sendAllLazy(request);
			else if (this.coordType.equals(CoordType.PAXOS)) {
				super.coordinateRequest(request);
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return true;
	}

	@Override
	public boolean createReplicaGroup(String serviceName, int epoch,
			String state, Set<String> nodes) {
		boolean created = false;
		if (this.coordType.equals(CoordType.LAZY)) {
			this.groups.put(serviceName, new CoordData(serviceName, epoch,
					nodes));
		} else if (this.coordType.equals(CoordType.PAXOS)) {
			created = super
					.createReplicaGroup(serviceName, epoch, state, nodes);
		}
		return created;
	}

	@Override
	public boolean deleteReplicaGroup(String serviceName, int epoch) {
		if (this.coordType.equals(CoordType.LAZY)) {
			// FIXME: check epoch here
			this.groups.remove(serviceName);
		} else if (this.coordType.equals(CoordType.PAXOS)) {
			super.deleteReplicaGroup(serviceName, epoch);
		}
		return true;
	}

	@Override
	public Set<String> getReplicaGroup(String serviceName) {
		if (this.coordType.equals(CoordType.LAZY)) {
			CoordData data = this.groups.get(serviceName);
			if (data != null)
				return data.replicas;
			else
				return null;
		} else
			assert (this.coordType.equals(CoordType.PAXOS));
		return super.getReplicaGroup(serviceName);
	}

	@Override
	public Set<IntegerPacketType> getRequestTypes() {
		return this.app.getRequestTypes();
	}

	@Override
	public ReconfigurableRequest getStopRequest(String name, int epoch) {
		if (this.app instanceof Reconfigurable)
			return ((Reconfigurable) this.app).getStopRequest(name,
					epoch);
		throw new RuntimeException(
				"Can not get stop request for a non-reconfigurable app");
	}

	// FIXME: unused method that exists only to prevent warnings
	protected boolean existsGroup(String name, int epoch) {
		CoordData data = this.groups.get(name);
		assert (data == null || data.name.equals(name));
		return (data != null && data.epoch == epoch);
	}

}
