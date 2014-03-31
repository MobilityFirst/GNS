package edu.umass.cs.gns.paxos;

import org.json.JSONObject;

import java.util.Set;

/**
 * Abstract class describing an interface for a PaxosManager.
 *
 * Created by abhigyan on 3/5/14.
 */
public abstract class AbstractPaxosManager {



  /**
   *
   * @param paxosIDNoVersion paxos ID of paxos instance excluding the version number
   * @param version number of times member set has been changed for this paxos ID
   * @param nodeIDs node IDs of all members
   * @param initialState initial state of data which this paxos instance is managing.
   * @return true if paxos instance is created. false if another instance with same ID and version already exists, or
   * size of nodeIDs is less than 3.
   */
  public abstract boolean createPaxosInstance(String paxosIDNoVersion, int version, Set<Integer> nodeIDs, PaxosInterface paxosInterface);

  /**
   * Propose requestPacket in the paxos instance with the given paxosID.
   *
   * ReqeustPacket.clientID is used to distinguish which method proposed this value.
   * @param paxosIDNoVersion paxosID of the paxos group excluding version number
   * @param value request to be proposed
   * @param stop true if this a stop request, false otherwise.
   * @return NULL if no paxos instance with given paxos ID was found, returns paxosIDNoVersion otherwise.
   */
  public abstract String propose(String paxosIDNoVersion, String value, boolean stop);


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
