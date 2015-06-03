package edu.umass.cs.reconfiguration;

import java.io.IOException;
import java.util.Set;

import edu.umass.cs.gigapaxos.InterfaceRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;


public interface InterfaceReplicaCoordinator<NodeIDType> {
	/* This method performs whatever replica coordination action is necessary to handle the request.
	 */
	public boolean coordinateRequest(
			InterfaceRequest request) throws IOException, RequestParseException;

	/* This method should return true if the replica group is successfully created or 
	 * one already exists with the same set of nodes. It should return false otherwise.
	 */
	public boolean createReplicaGroup(String serviceName, int epoch, String state, Set<NodeIDType> nodes);

	/* This method should result in all state corresponding to serviceName being deleted */
	public void deleteReplicaGroup(String serviceName, int epoch);

	/* This method must return the replica group that was most recently successfully created
	 * for the serviceName using createReplicaGroup.
	 */
	public Set<NodeIDType> getReplicaGroup(String serviceName);}
