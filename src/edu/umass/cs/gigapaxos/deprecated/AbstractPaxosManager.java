package edu.umass.cs.gigapaxos.deprecated;

import org.json.JSONObject;

import edu.umass.cs.gigapaxos.InterfaceReplicable;

import java.util.Set;

/* This abstract class is deprecated and was used mainly to transition from the 
 * old paxos implementation to the new one. It currently exists only for
 * backwards compatibility reasons.
 */
@Deprecated
public abstract class AbstractPaxosManager<NodeIdType> {

  /**
   *
   * @param paxosIDNoVersion paxos ID of paxos instance excluding the version number
   * @param version number of times member set has been changed for this paxos ID
   * @param nodeIDs node IDs of all members
   * @param paxosInterface initial state of data which this paxos instance is managing.
   * @return true if paxos instance is created. false if another instance with same ID and version already exists, or
   * size of nodeIDs is less than 3.
   */
  public abstract boolean createPaxosInstance(String paxosIDNoVersion, short version, Set<NodeIdType> nodeIDs, InterfaceReplicable paxosInterface);

  public abstract Set<NodeIdType> getPaxosNodeIDs(String paxosIDNoVersion);
  /**
   * Propose requestPacket in the paxos instance with the given paxosID.
   *
   * ReqeustPacket.clientID is used to distinguish which method proposed this value.
   * @param paxosIDNoVersion paxosID of the paxos group excluding version number
   * @param value request to be proposed
   * @return NULL if no paxos instance with given paxos ID was found, returns paxosIDNoVersion otherwise.
   */
  public abstract String propose(String paxosIDNoVersion, String value);

  public abstract String proposeStop(String paxosIDNoVersion, String value, short version);


  /**
   * Handle incoming message for any Paxos instance as well as failure detection messages.
   * @param json json object received
   */
  public abstract void handleIncomingPacket(JSONObject json);


  /**
   * Deletes all paxos instances and paxos logs.
   */
  public abstract void resetAll();


}
