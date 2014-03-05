package edu.umass.cs.gns.paxos;

import edu.umass.cs.gns.packet.paxospacket.RequestPacket;
import org.json.JSONObject;

import java.util.Set;

/**
 * Created by abhigyan on 3/5/14.
 */
public interface PaxosManagerInteface {

  /**
   *
   * @param paxosID paxos ID of paxos instance
   * @param nodeIDs node IDs of all members
   * @param initialState initial state of data which this paxos instance is managing.
   * @return true if paxos instance is created. false if another instance with same ID already exists, or
   * size of nodeIDs is less than 3.
   */
  public boolean createPaxosInstance(String paxosID, Set<Integer> nodeIDs, String initialState);

  /**
   * Propose requestPacket in the paxos instance with the given "paxos key". The paxos key for a given paxos ID
   * is defined by method {@link edu.umass.cs.gns.paxos.PaxosInterface#getPaxosKeyForPaxosID(String)}.
   *
   * The purpose of paxos key is to allow GNS to propose a request based only on the name, without knowing the
   * paxos ID for the current set of active replica.
   *
   * ReqeustPacket.clientID is used to distinguish which method proposed this value.
   * @param paxosKey paxosID of the paxos group
   * @param requestPacket request to be proposed
   */
  public String propose(String paxosKey, RequestPacket requestPacket);


  /**
   * Handle incoming message, incoming message could be of any Paxos instance.
   * @param json json obejct received
   */
  public void handleIncomingPacket(JSONObject json);
}
