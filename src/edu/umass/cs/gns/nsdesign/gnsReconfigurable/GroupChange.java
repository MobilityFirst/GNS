package edu.umass.cs.gns.nsdesign.gnsReconfigurable;

/**
 * On a change in the set of active replicas of a name, this class handle the transition from the old to the new
 * set of active replicas. To initiate the transition, a replica controller sends a message to one of the old active
 * replicas. After the transition is complete, that active replica will reply to the replica controller confirming
 * that the transition is complete.
 * <p>
 * The transition process depends on the implementation of {@link edu.umass.cs.gns.replicaCoordination.ActiveReplicaCoordinator}.
 * <p>
 * Created by abhigyan on 2/27/14.
 */
public class GroupChange {

}
