package edu.umass.cs.gns.paxos;

import edu.umass.cs.gns.paxos.paxospacket.*;
import org.json.JSONObject;

import java.util.Map;
import java.util.Set;

/**
 * Created by abhigyan on 1/13/14.
 * @param <NodeIDType>
 */
public abstract class PaxosReplicaInterface<NodeIDType>{

  public abstract String getPaxosID();

  public abstract boolean isNodeInPaxosInstance(NodeIDType responderNodeID);

  public abstract Set<NodeIDType> getNodeIDs();

  public abstract boolean isStopped();

  public abstract void handleIncomingMessage(JSONObject jsonObject, int packetType);

  public abstract void checkInitScout();

  public abstract void checkCoordinatorFailure();

  public abstract StatePacket getState();

  public abstract boolean isAcceptorBallotUpdated(Ballot ballot);

  public abstract boolean resendPrepare(Ballot proposedBallot, PreparePacket preparePacket);

  public abstract boolean resendPendingProposal(ProposalStateAtCoordinator propState);

  public abstract void removePendingProposals();


  /*** Methods related to recovering from logs ***/
  public abstract void recoverCurrentBallotNumber(Ballot ballot);

  public abstract void recoverSlotNumber(int slotNumber);

  public abstract void executeRecoveredDecisions();

  public abstract void recoverStop();

  public abstract void recoverDecision(ProposalPacket proposalPacket);

  public abstract void recoverPrepare(PreparePacket packet);

  public abstract void recoverAccept(AcceptPacket packet);



  /*** Methods used by class PaxosLogAnalayzer to compare logs at different nodes. Do not use it for
   * anything else ***/

  public abstract Map<Integer, PValuePacket> getPValuesAccepted();

  public abstract Map<Integer,RequestPacket> getCommittedRequests();

  public abstract int getSlotNumber();

  public abstract Ballot getAcceptorBallot();

}
