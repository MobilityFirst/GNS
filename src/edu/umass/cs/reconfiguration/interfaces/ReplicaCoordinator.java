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
package edu.umass.cs.reconfiguration.interfaces;

import java.io.IOException;
import java.util.Set;

import edu.umass.cs.gigapaxos.interfaces.Application;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

/**
 * @author arun
 *
 * @param <NodeIDType>
 */
public interface ReplicaCoordinator<NodeIDType> {
	/**
	 * This method performs whatever replica coordination action is necessary to
	 * handle the request.
	 * 
	 * @param request
	 * @return False if coordinated, else the result of local
	 *         {@link Application#execute} invocation.
	 * @throws IOException
	 * @throws RequestParseException
	 */
	public boolean coordinateRequest(Request request)
			throws IOException, RequestParseException;

	/**
	 * This method should return true if the replica group is successfully
	 * created or one already exists with the same set of nodes. It should
	 * return false otherwise.
	 * 
	 * @param serviceName
	 * @param epoch
	 * @param state
	 * @param nodes
	 * @return True if created successfully.
	 */
	public boolean createReplicaGroup(String serviceName, int epoch,
			String state, Set<NodeIDType> nodes);

	/**
	 * This method should result in all state corresponding to serviceName being
	 * deleted
	 * 
	 * @param serviceName
	 * @param epoch
	 * @return True if deleted without errors.
	 */
	public boolean deleteReplicaGroup(String serviceName, int epoch);

	/**
	 * This method must return the replica group that was most recently
	 * successfully created for the serviceName using createReplicaGroup.
	 * 
	 * @param serviceName
	 * @return Set of nodes in the replica group for {@code serviceName}.
	 */
	public Set<NodeIDType> getReplicaGroup(String serviceName);
}
