package edu.umass.cs.gns.reconfiguration;

/**
@author V. Arun
 */

/* FIXME: We probably need to move all methods involving an epoch
 * number below to AbstractReplicaCoordinator and relieve the app
 * from the burden of being epoch aware. The epoch related info--
 * name, epoch, replicas, isStopped, finalState--can be maintained
 * by AbstractReplicaCoordinator. This state is already maintained
 * by PaxosManager, but we need this info to be persistently
 * maintained for all coordinators.
 * 
 * One hack is to maintain a paxos manager object inside
 * AbstractReplicaCoordinator anyway but not rely upon it for 
 * coordinating requests unless the coordinator explicitly 
 * uses paxos, in which case it can share the paxos manager. An 
 * alternative is to use a separate database in 
 * AbstractReplicaCoordinator just for reconfiguration related
 * info just like the database at reconfigurators.
 */

public interface InterfaceReconfigurable extends InterfaceApplication {
	
	/* Returns a stop request that when executed by the app ensures that
	 * no subsequent request after the stop is executed in that epoch.
	 * We need the app to return a stop request as opposed to just
	 * a stop() method because, in general, the stop needs to be 
	 * coordinated across app replicas.
	 */
	public InterfaceReconfigurableRequest getStopRequest(String name, int epoch);

	public String getFinalState(String name, int epoch);

	public void putInitialState(String name, int epoch, String state);

	public boolean deleteFinalState(String name, int epoch);
	
	public Integer getEpoch(String name);
}
