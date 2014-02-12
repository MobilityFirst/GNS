package edu.umass.cs.gns.paxos;

import edu.umass.cs.gns.exceptions.GnsRuntimeException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartNameServer;
import edu.umass.cs.gns.nameserver.GenerateSyntheticRecordTable;
import edu.umass.cs.gns.packet.paxospacket.*;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * An implementation of the paxos protocol.
 *
 * Implementation is based on the document "Paxos Made Moderately Complex" by R van Renesse.
 * We have added the extension to the protocol made by Lamport in "Stoppable Paxos" to
 * be able to stop a paxos instance.
 *
 * User: abhigyan
 * Date: 6/26/13
 * Time: 12:09 AM
 */
public class PaxosReplica extends PaxosReplicaInterface{

  public static String Version = "$Revision$";

  /**
   * Special no-op command.
   */
  static final String NO_OP = "NO_OP";

  /**
   * ID of the paxos instance this replica belongs to.
   */
  private String paxosID;

  /**
   * ID of this replica.
   */
  private int nodeID;

  /**
   * IDs of nodes that belong to this paxos group.
   */
  private Set<Integer> nodeIDs;

  /**
   * One command is decided in each slot number. The command in {@code slotNumber}
   * will be run next. All commands until {@code slotNumber - 1} have been run by the replica.
   */
  private Integer slotNumber = 0;

  /**
   * Object used to synchronize access to {@code slotNumber}
   */
  private final ReentrantLock slotNumberLock = new ReentrantLock();

  /**
   * contains decisions for recent slots. K = slot number , v = request packet.
   * once a decision is accepted by all nodes, it is deleted from {@code decisions}
   */
  private ConcurrentHashMap<Integer, RequestPacket> decisions = new ConcurrentHashMap<Integer, RequestPacket>();

  /**
   * Ballot currently accepted by the replica. If this replica is coordinator,
   * then a separate object, {@code coordinatorBallot} stores the ballot used by coordinator
   */
  private Ballot acceptorBallot;

  /**
   * object used to synchronize access to {@code acceptorBallot}
   */
  private final ReentrantLock acceptorLock = new ReentrantLock();

  /**
   * Contains pValues that are accepted by this replica. When a slotNumber is
   */
  private ConcurrentHashMap<Integer, PValuePacket> pValuesAccepted  = new ConcurrentHashMap<Integer,PValuePacket>();

  /**
   * When did this replica last garbage collect its redundant state.
   */
  private long lastGarbageCollectionTime = 0;

  /**
   * Object used to synchronize access to {@code lastGarbageCollectionTime}.
   */
  private final ReentrantLock garbageCollectionLock = new ReentrantLock();

  private final ReentrantLock stopLock = new ReentrantLock();

  private boolean isStopped = false;


  /************ Coordinator variables *****************/

  /**
   * If activeCoordinator == true, this replica is coordinator.
   * otherwise I will drop any proposals that I receive.
   */
  private boolean activeCoordinator = false;

  /**
   * Coordinator has got {@code coordinatorBallot} accepted by majority of nodes.
   */
  private Ballot coordinatorBallot;

  /**
   * Lock controlling access to {@code coordinatorBallotLock}
   */
  private final ReentrantLock coordinatorBallotLock = new ReentrantLock();

  /**
   * {@code ballotScout} is the ballot currently proposed by coordinator
   * to get accepted by majority of replicas. When majority of replicas
   * accept {@code ballotScout}, {@code ballotScout} is copied to {@code coordinatorBallot} &
   * {@code ballotScout} is useless after that.
   */
  private Ballot ballotScout;

  /**
   * object used to synchronize access to {@code ballotScout}, {@code waitForScout},
   * and {@code pValuesScout}.
   */
  private final ReentrantLock scoutLock = new ReentrantLock();

  /**
   * List of replicas who have accepted the ballot {@code ballotScout}
   */
  private ArrayList<Integer> waitForScout;

  /**
   * Set of pValues received from replicas who have have accepted {@code ballotScout}.
   * Key = slot number, Value = PValuePacket, where PValuePacket contains the command that
   * was proposed by previous coordinator(s) for the slot number.
   *
   * A pValue is a tuple <slotNumber, ballot, request>. If multiple replicas respond
   * with a pValue for a given slot, the pValue with the highest ballot is accepted.
   * Once the ballot {@code ballotScout} is accepted by majority of replicas,
   * the coordinator proposes these commands again in the same slot numbers.
   */
  private HashMap<Integer,PValuePacket> pValuesScout;

  /**
   * Slot number for the next proposal by this coordinator.
   */
  private Integer nextProposalSlotNumber = 0;

  /**
   * Object synchronizing access to {@code nextProposalSlotNumber}
   */
  private final ReentrantLock proposalNumberLock = new ReentrantLock();

  /**
   * Stores information about commands that are currently proposed by
   * the coordinator, but not yet accepted. Key = slot number, Value = information
   * about command proposed in slot number.
   */
  private ConcurrentHashMap<Integer, ProposalStateAtCoordinator> pValuesCommander
          = new ConcurrentHashMap<Integer, ProposalStateAtCoordinator>();

  /**
   * Stores mapping from nodes and their current slot numbers. State before
   * minimum slot numbers across nodes can be garbage collected.
   */
  private final ConcurrentHashMap<Integer,Integer> nodeAndSlotNumbers = new ConcurrentHashMap<Integer, Integer>();

  // PAXOS-STOP
  /**
   * True if stop command is proposed by coordinator. Once stop command is decided in a slot,
   * the coordinator does not propose any commands in any higher numbered slots.
   */
  private boolean stopCommandProposed = false;


  private static final ReentrantLock minSlotLock = new ReentrantLock();

  private int minSlotNumberAcrossNodes = 0;

  /************ End of coordinator variables *****************/

  /**
   * Constructor.
   * @param paxosID ID of this paxos group
   * @param ID  ID of this node
   * @param nodeIDs set of IDs of nodes in this paxos group
   */
  public PaxosReplica(String paxosID, int ID, Set<Integer> nodeIDs) {
    this.paxosID = paxosID;
    this.nodeIDs = nodeIDs;
    this.nodeID = ID;
    acceptorBallot = new Ballot(0, getInitialCoordinatorReplica());

    coordinatorBallot  = new Ballot(0, getInitialCoordinatorReplica());
    if (coordinatorBallot.coordinatorID == nodeID) activeCoordinator = true;
  }


  /********************* START: Methods for recovery from paxos logs ********************************/


  /**
   * update acceptor ballot with ballot recovered from logs
   * @param b ballot number read from logs
   */
  public void recoverCurrentBallotNumber(Ballot b) {
    if (b == null) return;
    if (acceptorBallot == null || b.compareTo(acceptorBallot) > 0) {
      GNS.getLogger().fine(paxosID + "\t" + nodeID +
              " Paxos Recovery: Ballot updated to " + b);
      acceptorBallot = b;
    }
  }

  /**
   * recover current slot number from paxos logs.
   * @param slotNumber ballot number read from logs
   */
  public void recoverSlotNumber(int slotNumber) {
    if (slotNumber > this.slotNumber) {
      // update garbage collection slot
      GNS.getLogger().fine(paxosID + "\t" + nodeID +
              " Paxos Recovery: slotNumber updated to " + slotNumber);
      this.slotNumber = slotNumber;
    }
  }

  public void recoverDecision(ProposalPacket proposalPacket) {
    if (slotNumber <= proposalPacket.slot) {
      GNS.getLogger().fine(paxosID + "\t" + nodeID +
              " Paxos Recovery: added decision for slot " + proposalPacket.slot);
      decisions.put(proposalPacket.slot, proposalPacket.req);
    }
  }

  public void recoverAccept(AcceptPacket acceptPacket) {
    PValuePacket pValuePacket = acceptPacket.pValue;
    if (slotNumber <= pValuePacket.proposal.slot) {

      int compare = pValuePacket.ballot.compareTo(acceptorBallot);
      if (compare > 0) {
        acceptorBallot = pValuePacket.ballot;
        pValuesAccepted.put(pValuePacket.proposal.slot, pValuePacket);

      } else if (compare == 0) {
        pValuesAccepted.put(pValuePacket.proposal.slot, pValuePacket);
      }
      // else ignore if pvalue packet is less than current acceptor ballot
    }
  }

  public void recoverStop() {
    isStopped = true;
  }

  public void recoverPrepare(PreparePacket preparePacket) {
    if (preparePacket.ballot.compareTo(acceptorBallot) > 0) {
      acceptorBallot = preparePacket.ballot;
    }
  }

  public void executeRecoveredDecisions() {

    try{
      handleDecision(null, true);
      synchronized (coordinatorBallotLock) {
        activeCoordinator = false;
      }

    }catch (JSONException e) {
      e.printStackTrace();
    }

  }

  /********************* END: Methods for replica recovery from paxos logs ********************************/


  /********** BEGIN: Methods used by PaxosLogAnalyzer to compare logs of members of a group.
   * Do not use this methods for anything else. ************************/

  /**
   * Used only for testing!!
   * @return current acceptor ballot
   */
  public Ballot getAcceptorBallot() {
    return acceptorBallot;
  }

  /**
   * Used only for testing!!
   * @return current slot number
   */
  public int getSlotNumber() {
    return slotNumber;
  }

  /**
   * Used only for testing!!
   * @return  current pvalue object
   */
  public ConcurrentHashMap<Integer, PValuePacket> getPValuesAccepted() {
    return pValuesAccepted;
  }

  /**
   * Used only for testing!!
   * @return  current decisions stored at node
   */
  public ConcurrentHashMap<Integer, RequestPacket> getDecisions() {
    return decisions;
  }

  /********** END: Methods used by PaxosLogAnalyzer to compare logs of members of a group.
   * Do not use this methods for anything else. ************************/


  /********************* BEGIN: Public methods ********************************/

  /**
   * Handle an incoming message as per message type.
   * @param json json object received by this node
   */
  public void handleIncomingMessage(JSONObject json, int packetType) {

    try {
      if (isStopped) {
        GNS.getLogger().warning(paxosID +"\t Paxos instance = " + paxosID + " is stopped; message dropped.");
        return;
      }

      // client --> replica
      switch (packetType) {
        case PaxosPacketType.REQUEST:
          handleRequest(json);
          break;
        // replica --> coordinator
        case  PaxosPacketType.PROPOSAL:
          ProposalPacket proposal = new ProposalPacket(json);
          handleProposal(proposal);
          break;
        // coordinator --> replica
        case  PaxosPacketType.DECISION:
          proposal = new ProposalPacket(json);
          handleDecision(proposal,  false);
          break;
        // coordinator --> replica
        case PaxosPacketType.PREPARE:
          handlePrepare(json);
          break;
        // replica --> coordinator
        case PaxosPacketType.PREPARE_REPLY:
          PreparePacket prepare = new PreparePacket(json);
          handlePrepareMessageReply(prepare);
          break;
        // coordinator --> replica
        case PaxosPacketType.ACCEPT:
          handleAccept(json);
          break;
        // replica --> coordinator
        case PaxosPacketType.ACCEPT_REPLY:
          AcceptReplyPacket acceptReply = new AcceptReplyPacket(json);
          handleAcceptMessageReply(acceptReply);
          break;
        // replica --> replica
        // local failure detector --> replica
        case PaxosPacketType.NODE_STATUS:
          FailureDetectionPacket fdPacket = new FailureDetectionPacket(json);
          handleNodeStatusUpdate(fdPacket);
          break;
        case PaxosPacketType.SYNC_REQUEST:
          handleSyncRequest(new SynchronizePacket(json));
          break;
        case PaxosPacketType.SYNC_REPLY:
          handleSyncReplyPacket(new SynchronizeReplyPacket(json));
          break;
      }
    } catch (JSONException e) {
      GNS.getLogger().severe("JSON Exception here: " + "\t" + paxosID + "\t" + json + "\t" + packetType);
      e.printStackTrace();
    }
  }

  public String getPaxosID() {
    return paxosID;
  }

  /**
   *
   * @return node IDs of members of this group
   */
  public Set<Integer> getNodeIDs() {
    return nodeIDs;
  }

  /**
   * Check if a given node <code>nodeID</code> is in this Paxos instance.
   * @param nodeID  ID of node to be tested
   * @return true if node belongs to this paxos instance.
   */
  public  boolean isNodeInPaxosInstance(int nodeID) {
    return nodeIDs.contains(nodeID);
  }

  /**
   * is this paxos instance stopped?
   * @return true if stop command is executed
   */
  public boolean isStopped() {
    synchronized (stopLock) {
      return isStopped;
    }
  }

  public void checkInitScout() {
    boolean startScout = false;
    try {
      acceptorLock.lock();

      if (acceptorBallot.coordinatorID == nodeID) {
        startScout = true;
      } else if (!PaxosManager.isNodeUp(acceptorBallot.coordinatorID) && getNextCoordinatorReplica() == nodeID) {
        startScout = true;
      }
    } finally {
      acceptorLock.unlock();
    }
    if (startScout) initScout();
  }

  @Override
  public void removePendingProposals() {
    synchronized (stopLock) {
      isStopped = true; // ensures that when resendPendingProposals checks this replica,
      // it will know that replica is stopped, and requests wont be resent.
    }
  }

  public StatePacket getState() {

    try{
      acceptorLock.lock();
      synchronized (slotNumberLock) {
        String dbState = PaxosManager.clientRequestHandler.getState(paxosID);
        if (dbState == null) {
          GNS.getLogger().severe(paxosID + "\t" + nodeID + "\tError Exception Paxos state not logged because database state is null.");
          return null;
        }
        return  new StatePacket(acceptorBallot, slotNumber, dbState);
      }
    } finally {
      acceptorLock.unlock();
    }
  }

  /**
   * Received request from client, so propose it to the current coordinator.
   *
   * @param json request received as a json object
   * @throws JSONException
   */
  public void handleRequest(JSONObject json) throws JSONException{
    Ballot temp = null;
    try{
      acceptorLock.lock();
      if (acceptorBallot!=null) {
        temp = new Ballot(acceptorBallot.ballotNumber, acceptorBallot.coordinatorID);
      }
    } finally {
      acceptorLock.unlock();
    }

    json.put(ProposalPacket.SLOT, 0);
    json.put(PaxosPacketType.ptype, PaxosPacketType.PROPOSAL);
    json.put(PaxosManager.PAXOS_ID, paxosID);

    if (temp != null && PaxosManager.isNodeUp(temp.coordinatorID)) {
      PaxosManager.sendMessage(temp.coordinatorID,json, paxosID);
      GNS.getLogger().fine(paxosID + "\t" + nodeID +
              " Send proposal packet. Coordinator =  " + temp.coordinatorID + " Packet = " + json);
    } else {
      // if coordinator has failed, resend to other nodes who may be coordinators
      UpdateBallotTask ballotTask = new UpdateBallotTask(this, temp, json);
      PaxosManager.executorService.scheduleAtFixedRate(ballotTask, 0, PaxosManager.RESEND_PENDING_MSG_INTERVAL_MILLIS,
              TimeUnit.MILLISECONDS);
    }
  }

  public boolean isAcceptorBallotUpdated(Ballot ballot) {
    try {
      acceptorLock.lock();
      return acceptorBallot.compareTo(ballot) > 0;
    } finally {
      acceptorLock.unlock();
    }
  }


  public boolean resendPendingProposal(ProposalStateAtCoordinator state) {
    synchronized (stopLock) {
      if (isStopped) return false;
    }
//    if (state.getNodesResponded()*2 > nodeIDs.size()) return false;
    try {
      acceptorLock.lock();
      if (StartNameServer.debugMode)
        GNS.getLogger().info("\tResendingMessage\t" + paxosID + "\t" +
                state.pValuePacket.proposal.slot + "\t" + acceptorBallot + "\t");
      if (state.pValuePacket.ballot.compareTo(acceptorBallot) != 0) return false;
    }finally {
      acceptorLock.unlock();
    }

    int minSlot;
    synchronized (minSlotLock) {
      minSlot = minSlotNumberAcrossNodes;
    }

    for (int node: nodeIDs) {
      // ACCEPT-REPLY received from node?

      if (!state.hasNode(node)) {
        AcceptPacket accept = new AcceptPacket(this.nodeID, state.pValuePacket,
                PaxosPacketType.ACCEPT, minSlot);
//        GNS.getLogger().severe("\tResendingMessage\t" + state.paxosID + "\t" +
//                state.pValuePacket.proposal.slot + "\t" + acceptorBallot + "\t" + node);
        sendMessage(node, accept);
      }
    }
    return true;
  }

  /**************************END OF PUBLIC METHODS**********************************/


  /**
   * Send message p to replica {@code destID}.
   **/
  private void sendMessage(int destID, Packet p) {
    try
    {
      if (destID == nodeID) {
        handleIncomingMessage(p.toJSONObject(), p.packetType);
      }
      else {
        JSONObject json =  p.toJSONObject();
        PaxosManager.sendMessage(destID, json, paxosID);
      }
    } catch (JSONException e)
    {
      GNS.getLogger().fine(paxosID + "\t" +"JSON Exception. " + e.getMessage());
      e.printStackTrace();
    }
  }

  private int getInitialCoordinatorReplica() {

    String paxosID1 = paxosID;
    int index = paxosID.lastIndexOf("-");
    if (index > 0) paxosID1 = paxosID.substring(0, index);

    Random r = new Random(paxosID1.hashCode());
    ArrayList<Integer> x1  = new ArrayList<Integer>(nodeIDs);
    Collections.sort(x1);
    Collections.shuffle(x1, r);
    return x1.get(0);
  }

  private int getNextCoordinatorReplica() {

    String paxosID1 = paxosID;
    int index = paxosID.lastIndexOf("-");
    if (index > 0) paxosID1 = paxosID.substring(0, index);

    Random r = new Random(paxosID1.hashCode());
    ArrayList<Integer> x1  = new ArrayList<Integer>(nodeIDs);
    Collections.sort(x1);
    Collections.shuffle(x1, r);
    for (int x: x1) {
      if (PaxosManager.isNodeUp(x)) return x;
    }
    return  x1.get(0);
  }

  private void handleDecision(ProposalPacket prop, boolean recovery) throws JSONException{

    boolean stop = handleDecisionActual(prop, recovery);
    if (recovery) return;
    if (stop) {
      synchronized (PaxosManager.paxosInstances) {
        PaxosReplicaInterface r = PaxosManager.paxosInstances.get(PaxosManager.getPaxosKeyFromPaxosID(paxosID));
        if (r == null) return;
        if (r.getPaxosID().equals(paxosID)) {
          if (StartNameServer.debugMode) GNS.getLogger().fine("Paxos instance removed " + paxosID  + "\tReq ");
          PaxosManager.paxosInstances.remove(PaxosManager.getPaxosKeyFromPaxosID(paxosID));

        } else {
          if (StartNameServer.debugMode) GNS.getLogger().fine("Paxos instance already removed " + paxosID);
        }
      }
    }
  }

  /**
   * Handle decision response from coordinator.
   * @param prop decision received by node
   * @param recovery true if we are executing this decision while recovering from paxos logs
   * @throws JSONException
   */
  private boolean handleDecisionActual(ProposalPacket prop, boolean recovery) throws JSONException{
//    GNS.getLogger().fine("handling decision; " + paxosID + json);
    if (prop != null) {
      runGC(prop.gcSlot);

      if (StartNameServer.debugMode) GNS.getLogger().fine(paxosID + "\t" +nodeID +
              " Decision recvd at slot = " + prop.slot + " req = " + prop.req);

      decisions.put(prop.slot, prop.req);
    }

    boolean stop = false;

    while(true) {
      synchronized (slotNumberLock) {

        if (prop!= null && prop.slot == slotNumber) {
          if (prop.req.isStopRequest()) stop = true;
          perform(prop.req, slotNumber, recovery);
          slotNumber++;

        }
        else if (decisions.containsKey(slotNumber)) {
          if (decisions.get(slotNumber).isStopRequest()) stop = true;
          perform(decisions.get(slotNumber), slotNumber, recovery);
          slotNumber++;
        }
        else break;
      }

    }
    return stop;
  }

  /**
   * Apply the decision locally, e.g. write to disk.
   * @param req request to be executed
   * @param slotNumber slot number of current decision
   * @param recovery true if we are executing this decision while recovering from paxos logs
   * @throws JSONException
   */
  private void perform(RequestPacket req, int slotNumber, boolean recovery) throws JSONException{
    GNS.getLogger().fine("\tPAXOS-PERFORM\t" + paxosID + "\t" + nodeID + "\t" + slotNumber + "\t" + req.value);
    if (req.value.equals(NO_OP) ) {
      GNS.getLogger().fine(paxosID + "\t" +nodeID + " " + NO_OP + " decided in slot = " + slotNumber);
      return;
    }

    if (req.isStopRequest()) {
      synchronized (stopLock) {
        isStopped = true;
      }
      GNS.getLogger().fine(" Logging paxos stop " + req);
      PaxosLogger.logPaxosStop(paxosID);
    }

    PaxosManager.handleDecision(paxosID, req, recovery);

  }


  /**
   * New ballot proposed by a replica, handle the proposed ballot.
   * @param incomingJson json object received by node of type <code>PREPARE_PACKET</code>
   * @throws JSONException
   */
  private void handlePrepare(JSONObject incomingJson) throws JSONException {
//        PaxosLogger.logCurrentBallot(paxosID, packet.ballot);
    //
    PreparePacket packet = new PreparePacket(incomingJson);
    GNS.getLogger().fine(paxosID + "\t" +nodeID + " Acceptor received PreparePacket: " +
            packet.toJSONObject().toString());
//        Ballot b1 = null;
    try{
      acceptorLock.lock();
      if (acceptorBallot == null || packet.ballot.compareTo(acceptorBallot) > 0) {
        acceptorBallot = packet.ballot;

      }

      // Log this message and send reply to coordinator

      Packet p = packet.getPrepareReplyPacket(acceptorBallot, nodeID, pValuesAccepted, slotNumber);
      PaxosLogger.logMessage(new LoggingCommand(paxosID, incomingJson, LoggingCommand.LOG_AND_SEND_MSG,
              packet.coordinatorID, p.toJSONObject()));

    }finally {
      acceptorLock.unlock();
    }

  }


  /**
   * Handle accept messages from coordinator
   * @param incomingJson json object of type <code>ACCEPT_PACKET</code> received on node.
   */
  private void handleAccept(JSONObject incomingJson) throws JSONException{
    AcceptPacket accept = new AcceptPacket(incomingJson);
    AcceptReplyPacket acceptReply;

    try {
      GNS.getLogger().fine(paxosID + "\t" + nodeID + "\tAcceptorslot\t" + accept.pValue.proposal.slot);

      Ballot b1;
      try {
        acceptorLock.lock();
        if (accept.pValue.ballot.compareTo(acceptorBallot) == 0) { // accept the pvalue

          pValuesAccepted.put(accept.pValue.proposal.slot, accept.pValue);

          acceptReply = new AcceptReplyPacket(nodeID, acceptorBallot, accept.pValue.proposal.slot);

        } else if (accept.pValue.ballot.compareTo(acceptorBallot) > 0) {  // accept the pvalue and the ballot

          acceptorBallot = new Ballot(accept.pValue.ballot.ballotNumber, accept.pValue.ballot.coordinatorID);
          b1 = acceptorBallot;
//                    PaxosLogger.logCurrentBallot(paxosID, acceptorBallot);
          pValuesAccepted.put(accept.pValue.proposal.slot, accept.pValue);
          acceptReply = new AcceptReplyPacket(nodeID, b1,accept.pValue.proposal.slot);

        } else { // accept.pValue.ballot.compareTo(acceptorBallot) < 0)
          // do not accept, reply with own acceptor ballot
          b1 = new Ballot(acceptorBallot.ballotNumber, acceptorBallot.coordinatorID);
          acceptReply = new AcceptReplyPacket(nodeID, b1,accept.pValue.proposal.slot);

        }
        // new: log and send message.

        PaxosLogger.logMessage(new LoggingCommand(paxosID, incomingJson, LoggingCommand.LOG_AND_SEND_MSG,
                accept.nodeID, acceptReply.toJSONObject()));
      } finally {
        acceptorLock.unlock();
      }

    } catch (Exception e) {
      GNS.getLogger().fine("ACCEPTOR Exception here in acceptor");
      e.printStackTrace();
    }

  }

  private void runGC(int suggestedGCSlot) {

    synchronized (garbageCollectionLock) {
      long x = System.currentTimeMillis();
      if (x - lastGarbageCollectionTime > PaxosManager.MEMORY_GARBAGE_COLLECTION_INTERVAL) {
        lastGarbageCollectionTime = x;
      }
      else return;
    }

    int coordinatorID;
    try {
      acceptorLock.lock();
      coordinatorID = acceptorBallot.getCoordinatorID();
    } finally {
      acceptorLock.unlock();
    }

    deletePValuesDecided(suggestedGCSlot);
    deleteDecisionsAcceptedByAll(suggestedGCSlot);
    SynchronizeReplyPacket synchronizeReplyPacket = getSyncReply(false);
    sendMessage(coordinatorID, synchronizeReplyPacket);

  }

  /**
   * handle sync request message from a coordinator,
   * return the current missing slot numbers and maximum slot number for which decision is received
   * @param packet packet sent by coordinator
   */
  private void handleSyncRequest(SynchronizePacket packet) {
    SynchronizeReplyPacket synchronizeReplyPacket = getSyncReply(true);
    GNS.getLogger().fine(paxosID + "\t" +nodeID + " " + "Handling Sync Request: " + synchronizeReplyPacket);
    sendMessage(packet.nodeID, synchronizeReplyPacket);
  }

  /**
   * calculate the current missing slot numbers and maximum slot number for which decision is received
   * @return a SynchronizeReplyPacket to be sent to coordinator
   */
  private SynchronizeReplyPacket getSyncReply(boolean flag) {

    int maxDecision = -1;
    for (int slot: decisions.keySet()) {
      if (slot > maxDecision) {
        maxDecision = slot;
      }
    }

    int slot;
    synchronized (slotNumberLock) {
      slot = slotNumber;
    }

    ArrayList<Integer> missingSlotNumbers = new ArrayList<Integer>();
    for (int i = slot; i <= maxDecision; i++) {
      if (!decisions.containsKey(i))
        missingSlotNumbers.add(i);
    }

    return new SynchronizeReplyPacket(nodeID, maxDecision, missingSlotNumbers, flag);
  }

  /**
   * Delete pValues at slots less than slotNumber.
   * @param slotNumber all nodes have received all decisions for slots less than "slotNumber"
   *
   */
  private void deletePValuesDecided(int slotNumber) {
    GNS.getLogger().fine("Size of pvalues = " + pValuesAccepted.size());

    if (slotNumber == -1) {
      GNS.getLogger().fine(paxosID + "\t" +" Returning DELETE P values slot number = " + slotNumber);
      return;
    }
    GNS.getLogger().fine(paxosID + "\t" +"Entering delete P values. slot number = " + slotNumber);

    int minSlot = Integer.MAX_VALUE;
    for (int slot: pValuesAccepted.keySet()) {
      if (slot < minSlot) minSlot = slot;
    }
    for (int i = minSlot; i < slotNumber; i++) pValuesAccepted.remove(i);
  }

  /**
   * Delete decisions which have been performed at all nodes
   * @param slotNumber slot number up to which all nodes have applied decisions.
   */
  private void deleteDecisionsAcceptedByAll(int slotNumber) {

    int minSlot = Integer.MAX_VALUE;
    for (int slot: decisions.keySet()) {
      if (slot < minSlot) minSlot = slot;
    }
    for (int i = minSlot; i < slotNumber; i++) // < sign is very important
      decisions.remove(i);

  }



  /*****************BEGIN: Cooordinator-related private methods*****************/

  private int getNextProposalSlotNumber() {
    synchronized (proposalNumberLock) {
      if (stopCommandProposed) return -1;
      nextProposalSlotNumber += 1;
      return  nextProposalSlotNumber - 1;
    }
  }

  private void updateNextProposalSlotNumber(int slotNumber) {
    synchronized (proposalNumberLock) {
      if (stopCommandProposed) return;
      if (slotNumber > nextProposalSlotNumber) nextProposalSlotNumber = slotNumber;
    }
  }

  /**
   * Handle proposal for a new packet.
   * @throws JSONException
   */
  private void handleProposal(ProposalPacket p) throws JSONException {


    if (StartNameServer.debugMode)
      GNS.getLogger().fine(paxosID + "C\t" +nodeID +
              " Coordinator handling proposal: " + p.toJSONObject().toString());

    Ballot b = null;
    synchronized (coordinatorBallotLock) {
      if (activeCoordinator) {
        GNS.getLogger().fine(paxosID + "C\t" +nodeID +
                " Active coordinator yes: ");
        p.slot = getNextProposalSlotNumber();
        GNS.getLogger().fine(paxosID + "C\t" +nodeID + " Slot = " + p.slot);
        if (p.slot == -1) return;
        b = coordinatorBallot;
      }
      else {
        GNS.getLogger().fine(paxosID + "C\t" +nodeID +
                " Active coordinator no: ");
      }
    }
    if (b != null) {
      GNS.getLogger().fine(paxosID + "C\t" + nodeID + "C " +
              "Coordinator starting commander at slot = " + p.slot);
      initCommander(new PValuePacket(b, p));
    }

  }

  private int getMaxSlotNumberAcrossNodes() {
    synchronized (nodeAndSlotNumbers) {
      int max = 0;
      for (int x: nodeAndSlotNumbers.keySet()) {
        GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C\tNode = " + x + "\tSlot = " + nodeAndSlotNumbers.get(x));
        if (nodeAndSlotNumbers.get(x) > max) max = nodeAndSlotNumbers.get(x);
      }
      return max;
    }
  }

  /**
   * ballot adopted, coordinator elected, get proposed values accepted.
   * @throws JSONException
   */
  private void handleAdoptedNew() throws JSONException {

    GNS.getLogger().fine(paxosID + "C\t" + nodeID + "C Ballot adopted. Handling proposals. Paxos ID = " + paxosID +
            " Nodes = " + nodeIDs);

    // propose pValue received
    ArrayList<Integer> pValueSlots = new ArrayList<Integer>(pValuesScout.keySet());
    Collections.sort(pValueSlots);
    int stopCommandSlot = -1;

    ArrayList<PValuePacket> selectPValues = new ArrayList<PValuePacket>();
    for (int slot: pValueSlots) {
      if (pValuesScout.get(slot).proposal.req.isStopRequest()) {
        // check if it is to be executed
        boolean checkOutput = isStopCommandToBeProposed(slot);
        if (checkOutput) {
          // propose stop command
          selectPValues.add(new PValuePacket(ballotScout, pValuesScout.get(slot).proposal));
          stopCommandSlot = slot;
          break;
        }
        else {
          // remove pvalue at slot, so that no-op is proposed
          pValuesScout.remove(slot);
        }
      }
      else { // if (!r.isDecisionComplete(proposals.get(slot)))
        GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C " + "PValue proposal for slot: " + slot);
        selectPValues.add(new PValuePacket(ballotScout, pValuesScout.get(slot).proposal));
      }
    }
    GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C " + "Total PValue proposals : " + selectPValues.size());

    // propose no-ops in empty slots
    int maxSlotNumberAtReplica = getMaxSlotNumberAcrossNodes();
    GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C " + "Max slot number at replicas = "  + maxSlotNumberAtReplica);

    int maxPValueSlot = -1;
    for (int x: pValuesScout.keySet()) {
      if (x > maxPValueSlot) {
        maxPValueSlot = x;
      }
    }
    GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C " + "Received pValues for slots  = " + pValuesScout.keySet() +
            " max slot = " + maxPValueSlot);

    if (stopCommandSlot != -1 && stopCommandSlot < maxPValueSlot)
      maxPValueSlot = stopCommandSlot;

    ArrayList<Integer> noopSlots = new ArrayList<Integer>();
    // propose no-ops in slots for which no pValue is decided.
    for (int slot = maxSlotNumberAtReplica; slot <= maxPValueSlot; slot++) {
      if (!pValuesScout.containsKey(slot)) {
        // propose a no-op command for it
        noopSlots.add(slot);
      }
    }
    // clear the entries in pValuesScout.
    pValuesScout.clear();

    int temp = maxSlotNumberAtReplica;
    if (maxPValueSlot + 1 > temp) temp = maxPValueSlot + 1;
    updateNextProposalSlotNumber(temp);
    GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C Next proposal slot number is: " + temp);
    synchronized (coordinatorBallotLock) {
      coordinatorBallot = ballotScout;
      activeCoordinator = true;
      pValuesCommander.clear();
    }

    GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C " +
            " Now starting commanders for pvalues ...");
    for (PValuePacket p: selectPValues) {
      initCommander(p);
    }

    GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C " +
            " Now proposing no-ops for missing slots ...");
    for (int x: noopSlots) {
      proposeNoopInSlot(x, ballotScout);
    }

  }

  /**
   * On a STOP command in pValuesScout, check whether to execute it or not
   * @param stopSlot the slot at which stop command is executed.
   * @return false if the stop command is not to be executed,
   * true if coordinator should propose the stop command.
   */
  private boolean isStopCommandToBeProposed(int stopSlot) {
    Ballot stopBallot = pValuesScout.get(stopSlot).ballot;
    for (int x:pValuesScout.keySet()) {
      if (x > stopSlot) {
        // check whether this ballot is higher than ballot of stop slot
        Ballot xBallot = pValuesScout.get(x).ballot;
        // the ballot at this slot is higher than ballot at "stopSlot",
        // therefore "stopSlot" would not have been succesful.
        if(xBallot.compareTo(stopBallot) > 0) return false;
      }
    }
    return true;
  }

  /**
   * propose no-op command in given slot
   * @param slot slot number at which no-op command is to be proposed
   * @throws JSONException
   */
  private void proposeNoopInSlot(int slot, Ballot b) throws JSONException {
    ProposalPacket proposalPacket = new ProposalPacket(slot,
            new RequestPacket(0, PaxosReplica.NO_OP, PaxosPacketType.REQUEST, false),
            PaxosPacketType.PROPOSAL, 0);
    initCommander(new PValuePacket(b, proposalPacket));
    GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C Proposed NO-OP in slot = " + slot);
  }

  /**
   * Send message to acceptors with the proposed value and slot number.
   * @throws JSONException
   */
  private void initCommander(PValuePacket pValue) throws JSONException {
    // keep record of value
    ProposalStateAtCoordinator propState = new ProposalStateAtCoordinator(this, pValue, nodeIDs.size());

    pValuesCommander.put(pValue.proposal.slot, propState);
    PaxosManager.addToActiveProposals(propState);
    GNS.getLogger().fine(paxosID + "C\t" + nodeID +
            "C initialized commander values. Slot = " + pValue.proposal.slot);

    int minSlot;
    synchronized (minSlotLock) {
      minSlot = minSlotNumberAcrossNodes;
//            if (r.nextDouble() <= 0.001) System.out.println("nodes and slot numbers: " + nodeAndSlotNumbers);
    }
    if (pValue.proposal.req.isStopRequest()) {
      synchronized (proposalNumberLock){
        stopCommandProposed = true;
        GNS.getLogger().info(paxosID + "C\t" +nodeID + "C" +
                " stop command proposed. in slot = " + pValue.proposal.slot);
      }
    }
    AcceptPacket accept = new AcceptPacket(this.nodeID, pValue,
            PaxosPacketType.ACCEPT, minSlot);


    JSONObject jsonObject = accept.toJSONObject();
    PaxosManager.sendMessage(nodeIDs,jsonObject, paxosID);

  }

  private void handleSyncReplyPacket(SynchronizeReplyPacket replyPacket) {
    updateNodeAndSlotNumbers(replyPacket);

    if (replyPacket.missingSlotNumbers != null) {

      for (int x: replyPacket.missingSlotNumbers) {
        if (decisions.containsKey(x)) {

          RequestPacket decision = decisions.get(x);
          if (decision == null) continue;
          ProposalPacket packet = new ProposalPacket(x, decision, PaxosPacketType.DECISION, 0);

          sendMessage(replyPacket.nodeID, packet);
        }
        else {
          // currently we delete a request from memory only when all replicas have executed it
          // therefore this case will not arise.
          // TODO read from db and send state to node.

          GNS.getLogger().severe(paxosID + "C\t" + nodeID + "C " +
                  "Missing decision not found at coordinator for slot = " + x + " Decision: " + decisions.keySet());
        }
      }
    }
    if (replyPacket.flag) {
      // resend decisions after max decision slot
      for (int x: decisions.keySet()) {
        if (x > replyPacket.maxDecisionSlot) {
          RequestPacket decision = decisions.get(x);
          if (decision == null) continue;
          ProposalPacket packet = new ProposalPacket(x, decision, PaxosPacketType.DECISION, 0);
          sendMessage(replyPacket.nodeID, packet);
        }
      }
    }
  }

  /**
   * handle response of accept message from an acceptor.
   * @param accept  Accept message sent by a paxos replica.
   * @throws JSONException
   */
  private void handleAcceptMessageReply(AcceptReplyPacket accept) throws JSONException{
    // resend missing requests
//        updateNodeAndSlotNumbers(accept.nodeID, accept.slotNumberAtReplica);

    int slot = accept.slotNumber;
    Ballot b = accept.ballot;
    int senderNode = accept.nodeID; 
    GNS.getLogger().fine(paxosID + "C\t" + nodeID + "C Accept-Reply\tsender\t" + accept.nodeID
            + " slot\t" + slot);//+  "  accept = " + accept.toJSONObject().toString());
    ProposalStateAtCoordinator stateAtCoordinator = pValuesCommander.get(slot);

    if (stateAtCoordinator == null) {
      GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C " +
              "Commander not found for slot =  " + slot + ". Either decision complete or ballot preempted.");
      return;
    }

    if (b.compareTo(stateAtCoordinator.pValuePacket.ballot) <= 0) {
      // case: replica accepted proposed value
//                GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C ballot OK. ");
      stateAtCoordinator.addNode(senderNode);
//                GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C commander accept count: " +
//                        stateAtCoordinator.getNodesResponded() + " nodes "+ nodeIDs.size());
      sendDecisionForSlot(slot, stateAtCoordinator);
    }
    else if (b.compareTo(stateAtCoordinator.pValuePacket.ballot) > 0){
      // case: ballot preempted
      stateAtCoordinator = pValuesCommander.remove(slot);

      if (stateAtCoordinator != null) {
        PaxosManager.removeFromActiveProposals(stateAtCoordinator);
        GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C " + "higher ballot recvd. current ballot preempted.");
        try {
          acceptorLock.lock();
          synchronized (coordinatorBallotLock) {
            if (coordinatorBallot.compareTo(stateAtCoordinator.pValuePacket.ballot) == 0 && activeCoordinator) {
              activeCoordinator = false;
              coordinatorBallot = b;
              pValuesCommander.clear();
            }
          }
          if (stateAtCoordinator.pValuePacket.ballot.compareTo(acceptorBallot) > 0) {
            acceptorBallot = new Ballot(b.ballotNumber, b.coordinatorID);
          }
        }   finally {
          acceptorLock.unlock();
        }
        if (getNextCoordinatorReplica() == nodeID) initScout();
      }
    }

  }

  private void sendDecisionForSlot(int slot, ProposalStateAtCoordinator stateAtCoordinator) {
    // check if majority reached
    // FULLRESPONSE
//    if (stateAtCoordinator.isFullResponse()) {
//      PaxosManager.removeFromActiveProposals(stateAtCoordinator);
//      Object o = pValuesCommander.remove(slot);
//      if (o != null && StartNameServer.experimentMode) {
//        GNS.getLogger().info("\t" + paxosID + "\t" + slot + "\t" +
// stateAtCoordinator.pValuePacket.proposal.req.isStopRequest() + "\tFullResponse");
//      }
//      return;
//    }

    if (!stateAtCoordinator.isMajorityReached()) {
      return;
    }


    // remove object from pValuesCommander
    stateAtCoordinator = pValuesCommander.remove(slot);
    PaxosManager.removeFromActiveProposals(stateAtCoordinator);
    // if object deleted return
    if (stateAtCoordinator == null) return;

    // successfully removed object from pValuesCommander, so send.
//        GNS.getLogger().fine(paxosID + "C\t" + nodeID + "C " +
//                "commander received majority votes for slot = " + slot);
    // send decision to all replicas.

    stateAtCoordinator.pValuePacket.proposal.makeDecisionPacket();
//        ProposalPacket decision = stateAtCoordinator.pValuePacket.proposal.getDecisionPacket();

//        synchronized (coordinatorBallotLock) {
//            if (!activeCoordinator) {
//                pValuesCommander.clear();
//                return false;
//            }
//
//        }
    int minSlot;
    synchronized (minSlotLock) {
      minSlot = minSlotNumberAcrossNodes;
//            if (r.nextDouble() <= 0.001) System.out.println("nodes and slot numbers: " + nodeAndSlotNumbers);
    }

    try {
      stateAtCoordinator.pValuePacket.proposal.gcSlot = minSlot;
      JSONObject jsonObject = stateAtCoordinator.pValuePacket.proposal.toJSONObject();
      jsonObject.put(PaxosManager.PAXOS_ID, paxosID);
      PaxosManager.sendMessage(nodeIDs,jsonObject,paxosID);
//      handleIncomingMessage(jsonObject,);
//      if (PaxosManager.debug) {
//         PaxosManager.tcpTransport.sendToIDs(nodeIDs,jsonObject);
//      }
//      else {
//        for (int x: nodeIDs)
//          PaxosManager.sendMessage(x,jsonObject);
//      }
//    } catch (IOException e) {
//      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    } catch (JSONException e) {
      e.printStackTrace();
    }
//        GNS.getLogger().fine(paxosID + "C\t" + nodeID + "C " +
//                "Sending decision for slot = " + slot);

//        for (Integer i: nodeIDs) {
////            if (i != nodeID && r.nextDouble() < 0.20) {
//////            if (slot == 0) {
////                GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C " +
////                            "skip sending decision for slot = " + decision.slot);
////                continue;
////            }
//            sendMessage(i, stateAtCoordinator.pValuePacket.proposal);
////            GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C " +
////                    "sending decision for slot " + slot + " to " + i);
//        }
//    setLastDecisionTime();
//        GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C delete commander entries");
//        return true;
  }

  /**
   * Send prepare packet to get new ballot accepted.
   */
  private void initScout() {

    synchronized (proposalNumberLock) {
      if (stopCommandProposed) return;
    }

    try{
      scoutLock.lock();
      try{
        acceptorLock.lock();
//            synchronized (acceptorBallot) {
        synchronized (coordinatorBallotLock) {

//          if (activeCoordinator) { // if coordinator is currently activeCoordinator, dont choose new ballot.
////                        if (StartNameServer.debugMode) GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C Init scout skipped. already active." );
//            return;
//          }
          // scout will now choose a ballot
          if (ballotScout != null && ballotScout.compareTo(acceptorBallot) > 0) {
            ballotScout = new Ballot(ballotScout.ballotNumber + 1, nodeID);
          }
          else {
            ballotScout = new Ballot(acceptorBallot.ballotNumber + 1, nodeID);
          }
        }

      }finally {
        acceptorLock.unlock();
      }

      GNS.getLogger().severe(paxosID + "C\t" + nodeID + "C Coordinator proposing new ballot: " + ballotScout);

      waitForScout = new ArrayList<Integer>();
      pValuesScout = new HashMap<Integer,PValuePacket>();
    }finally {
      scoutLock.unlock();
    }

    // create prepare packet
    PreparePacket prepare = new PreparePacket(nodeID, 0, ballotScout, PaxosPacketType.PREPARE);

    for (Integer i: nodeIDs) {
      prepare.receiverID = i;
//            GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C sending prepare msg to " +i );
      sendMessage(i, prepare);
    }

    int numberRetry = 50;

    CheckPrepareMessageTask task = new CheckPrepareMessageTask(this,prepare, ballotScout, numberRetry);

    PaxosManager.executorService.scheduleAtFixedRate(task,PaxosManager.RESEND_PENDING_MSG_INTERVAL_MILLIS,
            PaxosManager.RESEND_PENDING_MSG_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);

  }

  /**
   * Returns true if there is a need to resend the prepare message again, otherwise false.
   * @param proposedBallot ballot that coordinator is proposing
   * @param prepare prepare packet sent to replicas
   * @return true if there is a need to resend the prepare message again, otherwise false.
   */
  public boolean resendPrepare(Ballot proposedBallot, PreparePacket prepare) {
    try {
      scoutLock.lock();
      if (proposedBallot.equals(ballotScout) && waitForScout != null) {
        // what if majority have failed?
        int nodesUp = 0;
        for (int x: nodeIDs)
          if (FailureDetection.isNodeUp(x)) nodesUp++;
        if (nodesUp*2 < nodeIDs.size()) return false; // more than half node down, give up resending prepare
        boolean resend = false;
        for (int x: nodeIDs) {
          if (!waitForScout.contains(x) && FailureDetection.isNodeUp(x)) {
            sendMessage(x, prepare); // send message to the node if it is up and has not responded.
            resend = true;
            GNS.getLogger().fine(paxosID + "\t" + nodeID + "C\t Ballot = " + proposedBallot + " resend to node " + x);
          }
        }
        return resend;
      } else {
        GNS.getLogger().severe(paxosID + "\t" + nodeID + "C\t No need to resend PreparePacket. Ballot adopted = " +
                proposedBallot);
        return false;
      }
    } finally {
      scoutLock.unlock();
    }

  }

  /**
   * Update <code>nodeAndSlotNumbers</code> to indicate that <code>nodeID</code> has executed
   * decisions until <code>slotNumber - 1</code>. Once all replicas have executed all decisions until a
   * slot number, coordinator informs replicas to delete state before that slot.
   * @param nodeID ID of node whose information is updated
   * @param slotNumber current slot number at that node
   */
  private void updateNodeAndSlotNumbers(int nodeID, int slotNumber) {

    if (!nodeAndSlotNumbers.containsKey(nodeID) ||
            slotNumber > nodeAndSlotNumbers.get(nodeID)) {
      nodeAndSlotNumbers.put(nodeID, slotNumber);
    }
  }

  /**
   * Updates nodeAndSlotNumbers based on replyPacket.
   * This helps coordinator track the current slot number at replicas. Once all replicas have executed
   * all decisions until a slot number, coordinator informs replicas to delete state before that slot.
   * @param  replyPacket packet received by coordinator from a replica
   */
  private void updateNodeAndSlotNumbers(SynchronizeReplyPacket replyPacket) {

    int minSlot = Integer.MAX_VALUE;
    if (replyPacket.missingSlotNumbers != null) {
      for (int x: replyPacket.missingSlotNumbers) {
        if (minSlot > x) minSlot = x;
      }
    }
    else {
      minSlot = replyPacket.maxDecisionSlot;
      minSlot += 1;
    }

    if (minSlot == Integer.MAX_VALUE) return;

    int node = replyPacket.nodeID;

    if (!nodeAndSlotNumbers.containsKey(node) || minSlot > nodeAndSlotNumbers.get(node)) {
            GNS.getLogger().finer(paxosID + "C\t" + node + "C node = " + node
                    + " Update: slot number = " + minSlot);
      nodeAndSlotNumbers.put(node, minSlot);
    }
    synchronized (minSlotLock){
      minSlotNumberAcrossNodes = getMinSlotNumberAcrossNodes();
    }

  }

  /**
   * handle reply from a replica for a prepare message
   * @throws JSONException
   */
  private void handlePrepareMessageReply(PreparePacket prepare) throws JSONException{

    updateNodeAndSlotNumbers(prepare.receiverID, prepare.slotNumber);

    try{
      scoutLock.lock();

      if (waitForScout == null) {
        GNS.getLogger().fine(paxosID + "C\t" + nodeID + "C not getting ballot accepted now.");
        return;
      }
      GNS.getLogger().finer(paxosID + "C\t" + nodeID
              + "C received Prepare-Reply Msg " + prepare.toJSONObject().toString());
      if (waitForScout.contains(prepare.receiverID)) {
        return;
      }

      if (prepare.ballot.compareTo(ballotScout) < 0) {
//                GNS.getLogger().fine(paxosID + "C\t" +nodeID
//                        + "C received response for a earlier scout. Ballot scout = " + ballotScout
//                        + " Message ballot = " + prepare.ballot);
        return;
      }
      else if (prepare.ballot.compareTo(ballotScout) > 0) {
        GNS.getLogger().fine(paxosID + "C\t" +nodeID  + "C Ballot pre-empted.");
        waitForScout = null;
      }
      else {
        GNS.getLogger().finer(paxosID + "C\t" + nodeID + "C Ballot accepted " +
                ballotScout + " by node " + prepare.receiverID);
        for(PValuePacket pval : prepare.accepted.values()) {
          int slot = pval.proposal.slot;
          if (!pValuesScout.containsKey(slot) ||
                  prepare.ballot.compareTo(pValuesScout.get(slot).ballot) > 0)  {
            pValuesScout.put(slot, pval);

          }
        }

        waitForScout.add(prepare.receiverID);
        if (waitForScout.size() > nodeIDs.size() / 2) { // activeCoordinator == false &&
          // case: ballot accepted by majority, cooridinator elected
          GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C " +
                  "Scout received majority votes for ballot. " + ballotScout + "Votes = "
                  + waitForScout.size() + " Nodes =  " + nodeIDs.size());
          waitForScout = null;
          handleAdoptedNew();
        }
      }
    }finally {
      scoutLock.unlock();
    }
  }

  /**
   * check whether coordinator is UP.
   */
  public void checkCoordinatorFailure() {
    int coordinatorID = -1;
    try{
      acceptorLock.lock();

      if (acceptorBallot != null ) {
        coordinatorID = acceptorBallot.coordinatorID;
      }
    } finally {
      acceptorLock.unlock();
    }

    if (!PaxosManager.isNodeUp(coordinatorID) && getNextCoordinatorReplica() == nodeID) {
      GNS.getLogger().severe(paxosID + "C\t" +nodeID +"C coordinator failed. " +
              coordinatorID);
      initScout();
    }
  }

  /**
   * message reporting node failure.
   */
  private void handleNodeStatusUpdate(FailureDetectionPacket packet) {
    if (packet.status) return; // node is up.
    try {
      assert packet.responderNodeID != nodeID;
    } catch (Exception e) {
      GNS.getLogger().severe("Exception Failure detection reports that this node has failed. Not possible. " + packet);
      e.printStackTrace();
      return;  // I have failed!! not possible.
    }

    if (StartNameServer.debugMode)
      GNS.getLogger().fine(paxosID + "C\t" +nodeID + " Node failed:"  + packet.responderNodeID);

    int coordinatorID = -1;
    try{
      acceptorLock.lock();

      if (acceptorBallot != null ) {
        synchronized (coordinatorBallotLock) {
          coordinatorID = coordinatorBallot.getCoordinatorID();
        }
      }
    } finally {
      acceptorLock.unlock();
    }
    if (packet.responderNodeID == coordinatorID // current coordinator has failed.
            && getNextCoordinatorReplica() == nodeID) { // I am next coordinator

      GNS.getLogger().warning(paxosID + "C\t" + nodeID + " Coordinator failed\t" + packet.responderNodeID +
              " Propose new ballot.");
      GNS.getLogger().severe(paxosID + "C\t" +nodeID +"C coordinator has failed " + coordinatorID);
      if (StartNameServer.debugMode)
        GNS.getLogger().fine(paxosID + "C\t" +nodeID + " current ballot coordinator failed " + coordinatorID);
      // I should try to get a new ballot accepted.
      initScout();
    }
  }

  /**
   * @return Minimum slot number in the list: nodeAndSlotNumbers across all replicas.
   */
  private int getMinSlotNumberAcrossNodes( ) {
    if (nodeAndSlotNumbers.size() < nodeIDs.size()) return -1;
    int minSlot = -1;
    for (int nodeID: nodeAndSlotNumbers.keySet()) {
//      if (FailureDetection.isNodeUp(nodeID) == false) continue;
      if (minSlot == -1 || minSlot > nodeAndSlotNumbers.get(nodeID))  {
        minSlot = nodeAndSlotNumbers.get(nodeID);
      }
    }
    return minSlot;
  }

  /*****************END: Cooordinator-related private methods*****************/

  static int getDefaultCoordinatorReplica(String paxosID, Set<Integer> nodeIDs) {
    String paxosID1 = PaxosManager.getPaxosKeyFromPaxosID(paxosID);
    Random r = new Random(paxosID1.hashCode());
    ArrayList<Integer> x1  = new ArrayList<Integer>(nodeIDs);
    Collections.sort(x1);
    Collections.shuffle(x1, r);
    return  x1.get(0);
  }

  public static void main (String[] args) throws IOException {
//    int numNS = 8;
//    int numNames = 1;
//
//    ConfigFileInfo.setNumberOfNameServers(numNS);
//
////    FileWriter fileWriter = new FileWriter("nameCoordinator.txt");
//    Set<Integer> actives = new HashSet<Integer>();
//    for (int i = 0; i < numNS; i++) {
//      actives.add(i);
//    }
//
//    for (int i = 0; i < numNames; i++) {
//      String name = Integer.toString(i);
////      Set<Integer> primaries = HashFunction.getPrimaryReplicasNoCache(name);
////      int[] actives = {68,1,69,2,71,5,66,67,7,8,9,77,10,12,74,15,23,22,25,28,31,30,34,35,37,40,47,44,52,62,61};
////      Set<Integer> primaries = new HashSet<Integer>();
////      for (int x: actives)
////        primaries.add(x);
//      int activeDefaultCoordinator = getDefaultCoordinatorReplica(name + "-1", actives);
//      System.out.println(name + "\t" + activeDefaultCoordinator + "\n");
////      fileWriter.write(name + "\t" + activeDefaultCoordinator + "\n");
////    int primaryDefaultCoordinator = getDefaultCoordinatorReplica(name + "-P", primaries);
////    System.out.println("Name\t" + name + "\tPrimaries\t" + primaries + "\tActiveCoordinator\t" +
////        activeDefaultCoordinator + "\tPrimaryCoordinator\t" + primaryDefaultCoordinator);
//    }
////    fileWriter.close();
//    System.exit(2);

//    String failedNamesFile = "/Users/abhigyan/Dropbox/gnrs/scripts/failed-names";
    String namedActivesFile = "/Users/abhigyan/Dropbox/gnrs/scripts/nameActives";

    HashMap<Integer, Set<Integer>> nameActives = GenerateSyntheticRecordTable.readActives(namedActivesFile);
//    BufferedReader br = new BufferedReader(new FileReader(failedNamesFile));
    HashMap<Integer, Integer> nodeCoordinators = new HashMap<Integer, Integer>();
    for (Integer nameInt: nameActives.keySet()) {
//    while (true) {
//      String name = br.readLine();
//      if (name == null) break;
      String name  = nameInt.toString();
      String paxosID = name.trim() + "-1";
//      int nameInt = Integer.parseInt(name);

      int defaultCoordinator = getDefaultCoordinatorReplica(paxosID, nameActives.get(nameInt));
//      System.out.println(name + "\t" + defaultCoordinator);
      if (nodeCoordinators.containsKey(defaultCoordinator)) nodeCoordinators.put(defaultCoordinator, nodeCoordinators.get(defaultCoordinator) + 1);
      else nodeCoordinators.put(defaultCoordinator,1);
    }
    HashMap<Integer,Integer> serverCount = new HashMap<Integer, Integer>();

    for (Integer nodeID: nodeCoordinators.keySet()) {
      if (serverCount.containsKey(nodeID%8)) serverCount.put(nodeID%8, serverCount.get(nodeID%8) + nodeCoordinators.get(nodeID));
      else serverCount.put(nodeID%8,nodeCoordinators.get(nodeID));
      System.out.println("\t" + nodeID + "\t" + nodeCoordinators.get(nodeID));
    }
    System.out.println(serverCount);
    System.exit(2);
  }

}

/**
 *
 */
class ProposalStateAtCoordinator implements Comparable, Serializable{


  public PaxosReplicaInterface paxosReplica;
  private long acceptSentTime = -1;
  PValuePacket pValuePacket;
  private ConcurrentHashMap<Integer, Integer> nodes;
  int numReplicas;

  public ProposalStateAtCoordinator(PaxosReplicaInterface paxosReplica, PValuePacket pValuePacket, int numReplicas) {

    this.paxosReplica = paxosReplica;
    this.pValuePacket = pValuePacket;
    this.nodes = new ConcurrentHashMap<Integer, Integer>();
    acceptSentTime = System.currentTimeMillis();
    this.numReplicas = numReplicas;
  }

  public void addNode(int node) {
    nodes.put(node,node);
  }

  private boolean majority = false;

  public synchronized boolean isMajorityReached() {
    if (majority)  return false;

    if (nodes.size() * 2 > numReplicas) {
      majority = true;
      return true;
    }
    return false;
  }

//  private boolean fullResponse = false;
//  public synchronized boolean isFullResponse() {
//    if (fullResponse || !majority)  return false;
//
//    if (nodes.size() == numReplicas) {
//      fullResponse = true;
//      return true;
//    }
//    return false;
//  }

  public boolean hasNode(int node) {
    return nodes.containsKey(node);
  }

  public long getTimeSinceAccept() {
    if (acceptSentTime == -1) acceptSentTime = System.currentTimeMillis();
    return  System.currentTimeMillis() - acceptSentTime;
  }

  @Override
  public int compareTo(Object o) {
    ProposalStateAtCoordinator p = (ProposalStateAtCoordinator)o;
    if (acceptSentTime < p.acceptSentTime) return -1;
    else if (acceptSentTime > p.acceptSentTime) return 1;
    int x =  paxosReplica.getPaxosID().compareTo(p.paxosReplica.getPaxosID());
    if (x != 0) return x;
    if (pValuePacket.proposal.slot < p.pValuePacket.proposal.slot) return -1;
    if (pValuePacket.proposal.slot > p.pValuePacket.proposal.slot) return 1;
    return 0;
  }

}

class CheckPrepareMessageTask extends TimerTask {

  int numRetry;
  int count = 0;
  PaxosReplicaInterface replica;
  PreparePacket preparePacket;
  Ballot proposedBallot;


  public CheckPrepareMessageTask(PaxosReplicaInterface replica, PreparePacket preparePacket, Ballot proposedBallot, int numRetry) {
    this.numRetry = numRetry;
    this.replica = replica;
    this.preparePacket = preparePacket;
    this.proposedBallot = proposedBallot;

  }

  @Override
  public void run() {
    try {
      if (replica.isStopped()) throw  new GnsRuntimeException();
      boolean sendAgain = replica.resendPrepare(proposedBallot,preparePacket);
      if (!sendAgain || count == numRetry) throw  new GnsRuntimeException();
    } catch (Exception e) {
      if (e.getClass().equals(GnsRuntimeException.class)) {
        throw new RuntimeException();
      }
      GNS.getLogger().severe("Exception in CheckPrepareMessageTask: " + e.getMessage());
      e.printStackTrace();
    }
  }
}

/**
 *
 */
class UpdateBallotTask extends TimerTask {

  PaxosReplicaInterface replica;

  Ballot ballot;

  int count = 0;

  ArrayList<Integer> nodes;

  JSONObject json;

  public UpdateBallotTask(PaxosReplicaInterface replica, Ballot ballot, JSONObject json) {
    this.replica = replica;
    this.ballot = ballot;
    String paxosID1 = PaxosManager.getPaxosKeyFromPaxosID(replica.getPaxosID()); //paxosID.split("-")[0];
    Random r = new Random(paxosID1.hashCode());
    nodes = new ArrayList<Integer>(replica.getNodeIDs());
    Collections.sort(nodes);
    Collections.shuffle(nodes, r);
    this.json = json;
  }

  @Override
  public void run() {
    if (count == nodes.size()) {
      throw new RuntimeException();
    }
    PaxosReplicaInterface pr = PaxosManager.paxosInstances.get(PaxosManager.getPaxosKeyFromPaxosID(replica.getPaxosID()));
    if (pr == null || !pr.getPaxosID().equals(replica.getPaxosID())) {
      throw new RuntimeException();
    }
    if (pr.isAcceptorBallotUpdated(ballot)) {
      throw new RuntimeException();
    }
    int node = -1;
    while(node == -1) {
      if (count < nodes.size() && PaxosManager.isNodeUp(nodes.get(count))) {
        node = nodes.get(count);
      }
      count += 1;
    }
    if (node > 0) {
      PaxosManager.sendMessage(node, json, replica.getPaxosID());
    }
  }
}