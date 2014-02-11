package edu.umass.cs.gns.paxos;

import edu.umass.cs.gns.packet.paxospacket.*;
import org.json.JSONObject;

import java.util.Map;
import java.util.Set;

/**
 * Created by abhigyan on 1/13/14.
 */
public abstract class PaxosReplicaInterface{

  public abstract void checkInitScout();

  public abstract void checkCoordinatorFailure();

  public abstract String getPaxosID();

  public abstract Ballot getAcceptorBallot();

  public abstract void handleIncomingMessage(JSONObject jsonObject, int packetType);

  public abstract boolean isNodeInPaxosInstance(int responderNodeID);

  public abstract boolean resendPendingProposal(ProposalStateAtCoordinator propState);

  public abstract StatePacket getState();

  public abstract void recoverCurrentBallotNumber(Ballot ballot);

  public abstract void recoverSlotNumber(int slotNumber);

  public abstract void executeRecoveredDecisions();

  public abstract void recoverStop();

  public abstract void recoverDecision(ProposalPacket proposalPacket);

  public abstract void recoverPrepare(PreparePacket packet);

  public abstract void recoverAccept(AcceptPacket packet);

  public abstract boolean isAcceptorBallotUpdated(Ballot ballot);

  public abstract boolean isStopped();

  public abstract boolean resendPrepare(Ballot proposedBallot, PreparePacket preparePacket);

  public abstract Set<Integer> getNodeIDs();

  public abstract void removePendingProposals();

  public abstract Map<Integer, PValuePacket> getPValuesAccepted();

  public abstract Map<Integer,RequestPacket> getDecisions();
}
