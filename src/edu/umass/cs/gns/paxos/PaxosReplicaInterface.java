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

  public abstract boolean isAcceptorBallotUpdated(Ballot<NodeIDType> ballot);

  public abstract boolean resendPrepare(Ballot<NodeIDType> proposedBallot, PreparePacket<NodeIDType> preparePacket);

  public abstract boolean resendPendingProposal(ProposalStateAtCoordinator<NodeIDType> propState);

  public abstract void removePendingProposals();


  /*** Methods related to recovering from logs
   * @param ballot ***/
  public abstract void recoverCurrentBallotNumber(Ballot<NodeIDType> ballot);

  public abstract void recoverSlotNumber(int slotNumber);

  public abstract void executeRecoveredDecisions();

  public abstract void recoverStop();

  public abstract void recoverDecision(ProposalPacket<NodeIDType> proposalPacket);

  public abstract void recoverPrepare(PreparePacket<NodeIDType> packet);

  public abstract void recoverAccept(AcceptPacket<NodeIDType> packet);



  /*** Methods used by class PaxosLogAnalayzer to compare logs at different nodes. Do not use it for
   * anything else
   * @return  ***/
  public abstract Map<Integer, PValuePacket<NodeIDType>> getPValuesAccepted();

  public abstract Map<Integer,RequestPacket<NodeIDType>> getCommittedRequests();

  public abstract int getSlotNumber();

  public abstract Ballot getAcceptorBallot();

}
