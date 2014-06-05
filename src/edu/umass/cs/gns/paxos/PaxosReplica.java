package edu.umass.cs.gns.paxos;

import edu.umass.cs.gns.exceptions.CancelExecutorTaskException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.paxos.paxospacket.*;
import edu.umass.cs.gns.util.ConsistentHashing;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * An implementation of the paxos protocol.
 * <p/>
 * <p/>
 * This class interfaces with PaxosManager class and PaxosLogger class. PaxosManager class is used to
 * provide (1) message transport to/from other nodes (2) execute a committed decision (e.g. a write operation)
 * (3) read current state of the data that this paxos replica is maintaining from the database.
 * (4) resend any requests proposed by coordinator that have failed to get response from majority after a timeout.
 * <p/>
 * PaxosLogger class is used to log paxos messages, e.g., PREPARE, COMMIT, ACCEPT. During recovery process,
 * PaxosLogger passes the messages recovered to PaxosReplica, which allows PaxosReplica to recreate its state.
 * <p/>
 * Terminology:
 * <p/>
 * * slot numbers (sequence numbers): set of integer value >= 0 (0, 1, 2, ..). all replicas executing paxos agrees on a
 * unique request to be executed in each slot. at any time, a replica would have executed requests in slots
 * (0, ..., S), where S is the last slot number.
 * <p/>
 * * commit: when a given request is decided to be executed in a given slot, it is said to be committed.
 * <p/>
 * * coordinator:  an elected leader among replicas which decides which requests to commit to what slot number.
 * <p/>
 * * ballot: as coordinators can change, a coordinator identifies itself by a ballot, which is the tuple (ballotNum, nodeID).
 * ballots are ordered, and a coordinator with higher ballot is regarded as the most recent coordinator.
 * <p/>
 * * prepare-message: to become a coordinator, a node send prepare message which includes its ballot to all replicas.
 * once majority of replicas reply to prepare-message a node becomes coordinator
 * <p/>
 * * accept-message: on each request , co-oordinator sends an accept-message to replicas to agree on order of requests.
 * <p/>
 * * decision: a message from coordinator to a replica indicating that a request is committed in a given slot.
 * <p/>
 * <p/>
 * Following this order in acquiring and releasing locks.
 * <code>scoutLock</code> <code>acceptorLock</code> <code>coordinatorLock</code>
 * <p/>
 * <p/>
 * Limitations of current implementation:
 * (1) We garbage collection state for slots numbers which are committed, but this requires that all replicas are up
 * (2)
 * <p/>
 * <p/>
 * Implementation is based on the document "Paxos Made Moderately Complex" by R van Renesse.
 * We have added the extension to the protocol made by Lamport in "Stoppable Paxos" to
 * be able to stop a paxos instance.
 * <p/>
 * User: abhigyan
 * Date: 6/26/13
 * Time: 12:09 AM
 */
public class PaxosReplica extends PaxosReplicaInterface implements Serializable {

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
   * will be run next. All commands until {@code slotNumber - 1} have been executed by the replica.
   */
  private Integer slotNumber = 0;

  /**
   * Object used to synchronize access to {@code slotNumber}
   */
  private final ReentrantLock slotNumberLock = new ReentrantLock();

  /**
   * contains committedRequests for recent slots. K = slot number , v = request packet.
   * once a decision is accepted by all nodes, it is deleted from {@code committedRequests}
   */
  private ConcurrentHashMap<Integer, RequestPacket> committedRequests = new ConcurrentHashMap<Integer, RequestPacket>();

  /**
   * Ballot currently accepted by the replica. If this replica is coordinator,
   * then a separate object, {@code coordinatorBallot} stores the ballot used by coordinator
   */
  private Ballot acceptorBallot;

  /**
   * Object used to synchronize access to {@code acceptorBallot}
   */
  private final ReentrantLock acceptorLock = new ReentrantLock();


  /**
   * A pValue is the content of the accept message which a coordinator sends to a replica on each request.
   * pValue includes a tuple (request, slotnumber, ballotnumber)
   * This hashmap contains pValues that are accepted by this replica.
   * HashMap key = slot number, value = pvalue received for that slot number.
   * <p/>
   * The hashmap entry for a slot number is deleted once all replicas have executed the request for that slot number.
   */
  private ConcurrentHashMap<Integer, PValuePacket> pValuesAccepted = new ConcurrentHashMap<Integer, PValuePacket>();

  /**
   * When did this replica last garbage collect its redundant state.
   * We garbage collect memeory state at every few hundred milliseconds, or when the replica
   * receives the next committed request from coordinator. Ssee parameter <code>MEMORY_GARBAGE_COLLECTION_INTERVAL</code>
   * in <code>PaxosManager</code>.
   */
  private long lastGarbageCollectionTime = 0;

  /**
   * Object used to synchronize access to {@code lastGarbageCollectionTime}.
   */
  private final ReentrantLock garbageCollectionLock = new ReentrantLock();


  /**
   * True is replica has executed the special STOP request, i.e., this replica will not execute any more request.
   */
  private boolean isStopped = false;


  /**
   * Object used to synchronize access to {@code isStopped}.
   */
  private final ReentrantLock stopLock = new ReentrantLock();


  /************ Coordinator variables. These are relevant only when replica is coordinator. *****************/

  /**
   * If activeCoordinator == true, this replica is coordinator.
   * otherwise I will drop any proposals that I receive.
   */
  private boolean activeCoordinator = false;

  /**
   * A coordinator is identified by {@code coordinatorBallot}.
   */
  private Ballot coordinatorBallot;

  /**
   * Lock controlling access to {@code coordinatorBallotLock}
   */
  private final ReentrantLock coordinatorBallotLock = new ReentrantLock();

  /**
   * To become a coordinator, this node has sent prepare message containing {@code ballotScout}
   * to all replicas . When majority of replicas reply that they agree on the
   * {@code ballotScout}, this node becoem coordinator. {@code ballotScout} is copied to {@code coordinatorBallot}
   * & {@code ballotScout} is useless after that.
   */
  private Ballot ballotScout;

  /**
   * Object used to synchronize access to {@code ballotScout}, {@code waitForScout},
   * and {@code pValuesScout}.
   */
  private final ReentrantLock scoutLock = new ReentrantLock();

  /**
   * List of replicas who have agreed on the ballot {@code ballotScout}
   */
  private ArrayList<Integer> waitForScout;

  /**
   * Set of pValues received from replicas who have have accepted {@code ballotScout}.
   * Key = slot number, Value = PValuePacket, where PValuePacket contains the command that
   * was proposed by previous coordinator(s) for the slot number.
   * <p/>
   * A pValue is a tuple <slotNumber, ballot, request>. If multiple replicas respond
   * with a pValue for a given slot, the pValue with the highest ballot is accepted.
   * Once the ballot {@code ballotScout} is accepted by majority of replicas,
   * the coordinator proposes these commands again in the same slot numbers.
   */
  private HashMap<Integer, PValuePacket> pValuesScout;

  /**
   * Slot number for the next request proposed by this coordinator.
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
  private ConcurrentHashMap<Integer, ProposalStateAtCoordinator> pValuesCommander = new ConcurrentHashMap<Integer, ProposalStateAtCoordinator>();

  // PAXOS-STOP
  /**
   * True if stop command is proposed by coordinator. Once stop command is decided in a slot,
   * the coordinator does not propose any commands in any higher numbered slots.
   */
  private boolean stopCommandProposed = false;


  /**
   * Stores mapping from nodes and their current slot numbers. State before
   * minimum slot numbers across nodes can be garbage collected.
   */
  private final ConcurrentHashMap<Integer, Integer> nodeAndSlotNumbers = new ConcurrentHashMap<Integer, Integer>();

  /**
   * This variable keeps track of minimimum value in <code>nodeAndSlotNumbers</code>.
   */
  private int minSlotNumberAcrossNodes = 0;

  /**
   * paxos manager object used for several things.
   */
  PaxosManager paxosManager;
  /**
   * Object synchronizing access to {@code minSlotNumberAcrossNodes}
   */
  private static final ReentrantLock minSlotLock = new ReentrantLock();

  private boolean debugMode = false;
  /************ End of coordinator variables *****************/

  /**
   * Constructor method. This method also initializes the default coordinator.
   *
   * @param paxosID ID of this paxos group
   * @param ID      ID of this node
   * @param nodeIDs set of IDs of nodes in this paxos group
   */
  public PaxosReplica(String paxosID, int ID, Set<Integer> nodeIDs, PaxosManager paxosManager) {
    this.paxosID = paxosID;
    this.nodeIDs = nodeIDs;
    this.nodeID = ID;
    this.paxosManager = paxosManager;
    // initialize paxos manager before initializing acceptorBallot and coordinatorBallot
    // because getInitialCoordinatorReplica depends on paxos manager
    // initialize acceptorBallot and coordinatorBallot to default values
    acceptorBallot = new Ballot(0, getInitialCoordinatorReplica());

    coordinatorBallot = new Ballot(0, getInitialCoordinatorReplica());
    debugMode = paxosManager.isDebugMode();
    if (coordinatorBallot.coordinatorID == nodeID) activeCoordinator = true;

    if (debugMode) GNS.getLogger().fine("Default coordinator chosen as: " + coordinatorBallot);
  }

  /********************* START: Methods for recovery from paxos logs ********************************/


  /**
   * update acceptor ballot with ballot recovered from logs
   *
   * @param b ballot number read from logs
   */
  public void recoverCurrentBallotNumber(Ballot b) {
    if (b == null) return;
    if (acceptorBallot == null || b.compareTo(acceptorBallot) > 0) {
      if (debugMode) GNS.getLogger().fine(paxosID + "\t" + nodeID +
              " Paxos Recovery: Ballot updated to " + b);
      acceptorBallot = b;
    }
  }

  /**
   * recover current slot number from paxos logs.
   *
   * @param slotNumber ballot number read from logs
   */
  public void recoverSlotNumber(int slotNumber) {
    if (slotNumber > this.slotNumber) {
      // update garbage collection slot
      if (debugMode) GNS.getLogger().fine(paxosID + "\t" + nodeID +
              " Paxos Recovery: slotNumber updated to " + slotNumber);
      this.slotNumber = slotNumber;
    }
  }

  public void recoverDecision(ProposalPacket proposalPacket) {
    if (slotNumber <= proposalPacket.slot) {
      if (debugMode) GNS.getLogger().fine(paxosID + "\t" + nodeID +
              " Paxos Recovery: added decision for slot " + proposalPacket.slot);
      committedRequests.put(proposalPacket.slot, proposalPacket.req);
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

    try {
      handleCommittedRequest(null, true);
      try {
        coordinatorBallotLock.lock();
        activeCoordinator = false;
      } finally {
        coordinatorBallotLock.unlock();
      }
    } catch (JSONException e) {
      e.printStackTrace();
    }

  }

  /********************* END: Methods for replica recovery from paxos logs ********************************/


  /********** BEGIN: Methods used by PaxosLogAnalyzer to compare logs of members of a group.
   * Do not use this methods for anything else. ************************/

  /**
   * Used only for testing!!
   *
   * @return current acceptor ballot
   */
  public Ballot getAcceptorBallot() {
    return acceptorBallot;
  }

  /**
   * Used only for testing!!
   *
   * @return current slot number
   */
  public int getSlotNumber() {
    return slotNumber;
  }

  /**
   * Used only for testing!!
   *
   * @return current pvalue object
   */
  public ConcurrentHashMap<Integer, PValuePacket> getPValuesAccepted() {
    return pValuesAccepted;
  }

  /**
   * Used only for testing!!
   *
   * @return current committedRequests stored at node
   */
  public ConcurrentHashMap<Integer, RequestPacket> getCommittedRequests() {
    return committedRequests;
  }

  /********** END: Methods used by PaxosLogAnalyzer to compare logs of members of a group.
   * Do not use this methods for anything else. ************************/


  /********************* BEGIN: Public methods ********************************/

  /**
   * Most common entry-point into this class.
   * Handle an incoming message as per message type.
   *
   * @param json json object received by this node
   */
  public void handleIncomingMessage(JSONObject json, int packetType) {

    try {
      if (isStopped) {
        GNS.getLogger().warning(paxosID + "\t Paxos instance = " + paxosID + " is stopped; message dropped.");
        return;
      }

      // client --> replica
      switch (PaxosPacketType.getPacketType(packetType)) {
        case REQUEST:
          handleRequest(json);
          break;
        // replica --> coordinator
        case PROPOSAL:
          ProposalPacket proposal = new ProposalPacket(json);
          handleProposal(proposal);
          break;
        // coordinator --> replica
        case DECISION:
          proposal = new ProposalPacket(json);
          handleCommittedRequest(proposal, false);
          break;
        // coordinator --> replica
        case PREPARE:
          handlePrepare(json);
          break;
        // replica --> coordinator
        case PREPARE_REPLY:
          PreparePacket prepare = new PreparePacket(json);
          handlePrepareMessageReply(prepare);
          break;
        // coordinator --> replica
        case ACCEPT:
          handleAccept(json);
          break;
        // replica --> coordinator
        case ACCEPT_REPLY:
          AcceptReplyPacket acceptReply = new AcceptReplyPacket(json);
          handleAcceptMessageReply(acceptReply);
          break;
        // replica --> replica
        // local failure detector --> replica
        case NODE_STATUS:
          FailureDetectionPacket fdPacket = new FailureDetectionPacket(json);
          handleNodeStatusUpdate(fdPacket);
          break;
        case SYNC_REQUEST:
          handleSyncRequest(new SynchronizePacket(json));
          break;
        case SYNC_REPLY:
          handleSyncReplyPacket(new SynchronizeReplyPacket(json));
          break;
        default:
          GNS.getLogger().severe("Received packet of type not found:" + json);
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
   * @return node IDs of members of this group
   */
  public Set<Integer> getNodeIDs() {
    return nodeIDs;
  }

  /**
   * Check if a given node <code>nodeID</code> is in this Paxos instance.
   *
   * @param nodeID ID of node to be tested
   * @return true if node belongs to this paxos instance.
   */
  public boolean isNodeInPaxosInstance(int nodeID) {
    return nodeIDs.contains(nodeID);
  }

  /**
   * Is this paxos instance stopped? Initially, isStopped is false,
   * after executing special STOP command, isStopped is set to true.
   *
   * @return true if stop command is executed
   */
  public boolean isStopped() {
    synchronized (stopLock) {
      return isStopped;
    }
  }

  /**
   * This method is called after this node has recovered the paxos state after a system crash.
   * It check if this node needs to propose a new ballot to become coordinator.
   * This happens in two cases: (1) if this node was a coordinator before the crash (2) the coordinator before
   * the crash has failed, an I am the next coordinator.
   */
  public void checkInitScout() {
    boolean startScout = false;
    try {
      acceptorLock.lock();

      if (acceptorBallot.coordinatorID == nodeID) {
        startScout = true;
      } else if (!paxosManager.isNodeUp(acceptorBallot.coordinatorID) && getNextCoordinatorReplica() == nodeID) {
        startScout = true;
      }
    } finally {
      acceptorLock.unlock();
    }
    if (startScout) initScout();
  }

  /**
   * This method is called in the special case of group change.
   * TODO complete documentation
   */
  public void removePendingProposals() {
    synchronized (stopLock) {
      isStopped = true; // ensures that when resendPendingProposals checks this replica,
      // it will know that replica is stopped, and requests wont be resent.
    }
  }

  /**
   * This method is called by logging module when it wants to periodically log the complete state for paxos
   * instance to disk. This methods return the state for paxos instance consisting of (1) Database state
   * (2) current slot number (3) ballot of current coordinator.
   *
   * @return Current state of this paxos replica.
   */
  public StatePacket getState() {

    try {
      acceptorLock.lock();
      synchronized (slotNumberLock) {
        String dbState = paxosManager.clientRequestHandler.getState(paxosID);
        if (dbState == null) {
          GNS.getLogger().severe(paxosID + "\t" + nodeID + "\tError Exception Paxos state not logged because database state is null.");
          return null;
        }
        return new StatePacket(acceptorBallot, slotNumber, dbState);
      }
    } finally {
      acceptorLock.unlock();
    }
  }


  /**
   * To understand this method refer to documentation on method <code>handleRequest</code> and
   * <code>UpdateBallotTask</code>.
   *
   * @param ballot
   * @return
   */
  public boolean isAcceptorBallotUpdated(Ballot ballot) {
    try {
      acceptorLock.lock();
      return acceptorBallot.compareTo(ballot) > 0;
    } finally {
      acceptorLock.unlock();
    }
  }

  /**
   * Also see the documentation for <code>ResendPendingMessagesTask</code> to understand this method.
   * <p/>
   * This method is called by <code>ResendPendingMessagesTask</code> class. It requests
   * the coordinator to resend a proposed request which has not yet received accept replies from majority
   * of replicas. The method returns true if the coordinator resends the message, false otherwise.
   * 'false' is return either when (1) replica is stopped or (2) coordinator ballot has changed, e.g., due to new
   * coordinator election.
   *
   * @param state the <code>ProposalStateAtCoordinator</code> which contains state for the proposal
   * @return true if
   */
  public boolean resendPendingProposal(ProposalStateAtCoordinator state) {
    synchronized (stopLock) {
      if (isStopped) return false;
    }
    try {
      acceptorLock.lock();

      GNS.getLogger().info("\tResendingMessage\t" + paxosID + "\t" + state.pValuePacket.proposal.slot +
              "\t" + acceptorBallot + "\t");
      if (state.pValuePacket.ballot.compareTo(acceptorBallot) != 0) return false;
    } finally {
      acceptorLock.unlock();
    }

    int minSlot;
    synchronized (minSlotLock) {
      minSlot = minSlotNumberAcrossNodes;
    }

    for (int node : nodeIDs) {
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
   * This method converts packet to JSON object and calls paxosManager to actually send.
   * If destination node is this replica itself, it calls <code>handleIncomingMessage</>
   * to handle the message.
   * <p/>
   * DO NOT call this method from inside from synchronized code block, becasue
   * it could call <code>handleIncomingMessage</code>, which again tries to acquire locks on some objects in this class.
   */
  private void sendMessage(int destID, PaxosPacket p) {
    try {
      if (destID == nodeID) {
        handleIncomingMessage(p.toJSONObject(), p.packetType);
      } else {
        JSONObject json = p.toJSONObject();
        paxosManager.sendMessage(destID, json, paxosID);
      }
    } catch (JSONException e) {
      GNS.getLogger().fine(paxosID + "\t" + "JSON Exception. " + e.getMessage());
      e.printStackTrace();
    }
  }

  /**
   * Computes the default coordinator node for this paxos group among the set of nodes.
   * The default coordinator depends on the string <code>paxosID</code>.
   * <p/>
   * We sort the set of nodes in a deterministic fashion and take the first node in the list.
   *
   * @return
   */
  private int getInitialCoordinatorReplica() {
    if (paxosManager.isConsistentHashCoordinatorOrder()) return getConsistentHashCoordinatorOrder().get(0);
    return getDefaultCoordinatorOrder().get(0);
  }

  /**
   * Computes the next coordinator node for this paxos group among the set of nodes.
   * <p/>
   * We sort the set of nodes in a deterministic fashion and take the first node in the list that is not failed.
   * This method depends on failure detector to tell whether a node is failed.
   *
   * @return nodeID of next coordinator replica
   */
  private int getNextCoordinatorReplica() {
    ArrayList<Integer> x1;
    if (paxosManager.isConsistentHashCoordinatorOrder()) x1 = getConsistentHashCoordinatorOrder();
    else x1 = getDefaultCoordinatorOrder();

    for (int x : x1) {
      if (paxosManager.isNodeUp(x)) return x;
    }
    return x1.get(0);
  }

  /**
   * Computes the default ordering among nodes to be elected as coordinator.
   */
  private ArrayList<Integer> getDefaultCoordinatorOrder() {

    String paxosID1 = paxosManager.getPaxosKeyFromPaxosID(paxosID);
    Random r = new Random(paxosID1.hashCode());
    ArrayList<Integer> x1 = new ArrayList<Integer>(nodeIDs);
    Collections.sort(x1);
    Collections.shuffle(x1, r);
    return x1;
  }

  /**
   * Computes the ordering among nodes based on a consistent hash of their node IDs. Nodes are sorted in a circular
   * order with the first node as the node whose consistent hash is greater or equal to the paxos ID.
   */
  private ArrayList<Integer> getConsistentHashCoordinatorOrder() {
    GNS.getLogger().info("Using consistent-hash based coordinator");
    TreeMap<String, Integer> sorted = new TreeMap<String, Integer>();
    for (int x : nodeIDs) {
      String hashVal = ConsistentHashing.getConsistentHash(Integer.toString(x));
      sorted.put(hashVal, x);
    }
    // assuming paxos ID is the hash of the name
    String paxosIDHash = paxosManager.getPaxosKeyFromPaxosID(paxosID);
    ArrayList<Integer> sorted1 = new ArrayList<Integer>();
    for (String nodeHash : sorted.keySet()) {
      if (nodeHash.compareTo(paxosIDHash) >= 0) sorted1.add(sorted.get(nodeHash));
    }
    for (String nodeHash : sorted.keySet()) {
      if (nodeHash.compareTo(paxosIDHash) >= 0) break;
      sorted1.add(sorted.get(nodeHash));
    }
    return sorted1;
  }

  /**
   * Received request from client, so propose it to the current coordinator.
   * <p/>
   * If current coordinator is detected failed, we try to find out the current coordinator node by contacting other replicas.
   * This method creates a task to obtain the new coordinator.
   *
   * @param json request received as a json object
   * @throws JSONException
   */
  private void handleRequest(JSONObject json) throws JSONException {
    Ballot temp = null;
    try {
      acceptorLock.lock();
      if (acceptorBallot != null) {
        temp = new Ballot(acceptorBallot.ballotNumber, acceptorBallot.coordinatorID);
      }
    } finally {
      acceptorLock.unlock();
    }

    json.put(ProposalPacket.SLOT, 0);
    json.put(PaxosPacket.PACKET_TYPE_FIELD_NAME, PaxosPacketType.PROPOSAL.getInt());
    json.put(PaxosManager.PAXOS_ID, paxosID);

    if (temp != null && paxosManager.isNodeUp(temp.coordinatorID)) {
      paxosManager.sendMessage(temp.coordinatorID, json, paxosID);
      if (debugMode) GNS.getLogger().fine(paxosID + "\t" + nodeID +
              " Send proposal packet. Coordinator =  " + temp.coordinatorID + " Packet = " + json);
    } else {
      // if coordinator has failed, resend to other nodes who may be coordinators
      UpdateBallotTask ballotTask = new UpdateBallotTask(paxosManager, this, temp, json);
      paxosManager.executorService.scheduleAtFixedRate(ballotTask, 0, PaxosManager.RESEND_PENDING_MSG_INTERVAL_MILLIS,
              TimeUnit.MILLISECONDS);
    }
  }

  /**
   * Coordinator has sent a message to replica to execute a committed request.
   * <p/>
   * If the committed request is a STOP request, this method also removes the current PaxosReplica objects
   * from the list of paxos instances maintained by the coordinator.
   * <p/>
   * As a special case, this method is also called during log recovery after a crash to execute requests
   * recovered from logs
   *
   * @param prop     ProposalPacket containing the committed request.
   * @param recovery true if this method is called during log recovery after a crash, false otherwise
   * @throws JSONException
   */
  private void handleCommittedRequest(ProposalPacket prop, boolean recovery) throws JSONException {

    boolean stop = handleDecisionActual(prop, recovery);
    if (recovery) return;
    if (stop) {
      synchronized (paxosManager.paxosInstances) {
        PaxosReplicaInterface r = paxosManager.paxosInstances.get(paxosManager.getPaxosKeyFromPaxosID(paxosID));
        if (r == null) return;
        if (r.getPaxosID().equals(paxosID)) {
          if (debugMode) GNS.getLogger().fine("Paxos instance removed " + paxosID + "\tReq ");
          paxosManager.paxosInstances.remove(paxosManager.getPaxosKeyFromPaxosID(paxosID));

        } else {
          if (debugMode) GNS.getLogger().fine("Paxos instance already removed " + paxosID);
        }
      }
    }
  }

  /**
   * This method is called by <code>handleCommittedRequest</code> to execute the decision.
   * <p/>
   * This method adds committed request to list of decisions <code>committedRequests</code>.
   * If the replica has already executed requests until say slot = x, and the replica has received committed requests
   * from slots (x+1) to y, where (x+1) <= y, then this methods executed all requests in slot (x+1) to y.
   * <p/>
   * The committed request is executed only if this replica has already executed all requests before
   *
   * @param prop     decision received by node
   * @param recovery true if we are executing this decision while recovering from paxos logs
   * @throws JSONException
   */
  private boolean handleDecisionActual(ProposalPacket prop, boolean recovery) throws JSONException {
    if (prop != null) {
      runGC(prop.gcSlot);

      if (debugMode) GNS.getLogger().fine(paxosID + "\t" + nodeID +
              " Decision recvd at slot = " + prop.slot + " req = " + prop.req);

      committedRequests.put(prop.slot, prop.req);
      // log and do nothing
      paxosManager.paxosLogger.logMessage(new LoggingCommand(paxosID, prop.toJSONObject(), LoggingCommand.LOG_AND_SEND_MSG));
    }

    boolean stop = false;

    while (true) {
      synchronized (slotNumberLock) {

        if (prop != null && prop.slot == slotNumber) {
          if (prop.req.isStopRequest()) stop = true;
          perform(prop.req, slotNumber, recovery);
          slotNumber++;

        } else if (committedRequests.containsKey(slotNumber)) {
          if (committedRequests.get(slotNumber).isStopRequest()) stop = true;
          perform(committedRequests.get(slotNumber), slotNumber, recovery);
          slotNumber++;
        } else break;
      }
    }
    return stop;
  }

  /**
   * Apply the decision locally, e.g. write to disk.
   *
   * @param req        request to be executed
   * @param slotNumber slot number of current decision
   * @param recovery   true if we are executing this decision while recovering from paxos logs
   * @throws JSONException
   */
  private void perform(RequestPacket req, int slotNumber, boolean recovery) throws JSONException {
    if (debugMode)
      GNS.getLogger().fine("\tPAXOS-PERFORM\t" + paxosID + "\t" + nodeID + "\t" + slotNumber + "\t" + req.value);
    if (req.value.equals(NO_OP)) {
      if (debugMode) GNS.getLogger().fine(paxosID + "\t" + nodeID + " " + NO_OP + " decided in slot = " + slotNumber);
      return;
    }

    if (req.isStopRequest()) {
      synchronized (stopLock) {
        isStopped = true;
      }
      if (debugMode) GNS.getLogger().fine(" Logging paxos stop " + req);
      paxosManager.paxosLogger.logPaxosStop(paxosID);
    }
//    if (debugMode) GNS.getLogger().info("executing perform at " + nodeID);
    paxosManager.handleDecision(paxosID, req, recovery);

  }

  /**
   * New ballot proposed by a replica, handle the proposed ballot.
   *
   * @param incomingJson json object received by node of type <code>PREPARE_PACKET</code>
   * @throws JSONException
   */
  private void handlePrepare(JSONObject incomingJson) throws JSONException {
//        PaxosLogger.logCurrentBallot(paxosID, packet.ballot);
    //
    PreparePacket packet = new PreparePacket(incomingJson);
    if (debugMode) GNS.getLogger().fine(paxosID + "\t" + nodeID + " Acceptor received PreparePacket: " +
            packet.toJSONObject().toString());
//        Ballot b1 = null;
    try {
      acceptorLock.lock();
      if (acceptorBallot == null || packet.ballot.compareTo(acceptorBallot) > 0) {
        acceptorBallot = packet.ballot;

      }

      // Log this message and send reply to coordinator

      PaxosPacket p = packet.getPrepareReplyPacket(acceptorBallot, nodeID, pValuesAccepted, slotNumber);
      paxosManager.paxosLogger.logMessage(new LoggingCommand(paxosID, incomingJson, LoggingCommand.LOG_AND_SEND_MSG,
              packet.coordinatorID, p.toJSONObject()));

    } finally {
      acceptorLock.unlock();
    }

  }


  /**
   * Handle accept messages from coordinator
   *
   * @param incomingJson json object of type <code>ACCEPT_PACKET</code> received on node.
   */
  private void handleAccept(JSONObject incomingJson) throws JSONException {
    AcceptPacket accept = new AcceptPacket(incomingJson);
    AcceptReplyPacket acceptReply;

    try {
      if (debugMode) GNS.getLogger().fine(paxosID + "\t" + nodeID + "\tAcceptorslot\t" + accept.pValue.proposal.slot);

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
          acceptReply = new AcceptReplyPacket(nodeID, b1, accept.pValue.proposal.slot);

        } else { // accept.pValue.ballot.compareTo(acceptorBallot) < 0)
          // do not accept, reply with own acceptor ballot
          b1 = new Ballot(acceptorBallot.ballotNumber, acceptorBallot.coordinatorID);
          acceptReply = new AcceptReplyPacket(nodeID, b1, accept.pValue.proposal.slot);

        }
        // new: log and send message.

        paxosManager.paxosLogger.logMessage(new LoggingCommand(paxosID, incomingJson, LoggingCommand.LOG_AND_SEND_MSG,
                accept.nodeID, acceptReply.toJSONObject()));
      } finally {
        acceptorLock.unlock();
      }

    } catch (Exception e) {
      GNS.getLogger().fine("ACCEPTOR Exception here in acceptor");
      e.printStackTrace();
    }

  }

  /**
   * Garbage collect all slots less than <code>suggestedGCSlot</code>.
   * Do not run garbage collection if it was run less than
   * <code>MEMORY_GARBAGE_COLLECTION_INTERVAL</code> algo.
   *
   * @param suggestedGCSlot state at slots less than this are garbage collected
   */
  private void runGC(int suggestedGCSlot) {

    synchronized (garbageCollectionLock) {
      long x = System.currentTimeMillis();
      if (x - lastGarbageCollectionTime > PaxosManager.MEMORY_GARBAGE_COLLECTION_INTERVAL) {
        lastGarbageCollectionTime = x;
      } else return;
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
   * Handle sync request message from a coordinator,
   * return the current missing slot numbers and maximum slot number for which decision is received
   *
   * @param packet packet sent by coordinator
   */
  private void handleSyncRequest(SynchronizePacket packet) {
    SynchronizeReplyPacket synchronizeReplyPacket = getSyncReply(true);
    if (debugMode)
      GNS.getLogger().fine(paxosID + "\t" + nodeID + " " + "Handling Sync Request: " + synchronizeReplyPacket);
    sendMessage(packet.nodeID, synchronizeReplyPacket);
  }

  /**
   * Calculate the current missing slot numbers and maximum slot number for which decision is received
   *
   * @return a SynchronizeReplyPacket to be sent to coordinator
   */
  private SynchronizeReplyPacket getSyncReply(boolean flag) {

    int maxDecision = -1;
    for (int slot : committedRequests.keySet()) {
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
      if (!committedRequests.containsKey(i))
        missingSlotNumbers.add(i);
    }

    return new SynchronizeReplyPacket(nodeID, maxDecision, missingSlotNumbers, flag);
  }

  /**
   * Delete pValues at slots less than slotNumber.
   *
   * @param slotNumber all nodes have received all committedRequests for slots less than "slotNumber"
   */
  private void deletePValuesDecided(int slotNumber) {
    if (debugMode) GNS.getLogger().fine("Size of pvalues = " + pValuesAccepted.size());

    if (slotNumber == -1) {
      if (debugMode) GNS.getLogger().fine(paxosID + "\t" + " Returning DELETE P values slot number = " + slotNumber);
      return;
    }
    if (debugMode) GNS.getLogger().fine(paxosID + "\t" + "Entering delete P values. slot number = " + slotNumber);

    int minSlot = Integer.MAX_VALUE;
    for (int slot : pValuesAccepted.keySet()) {
      if (slot < minSlot) minSlot = slot;
    }
    for (int i = minSlot; i < slotNumber; i++) pValuesAccepted.remove(i);
  }

  /**
   * Delete committedRequests which have been performed at all nodes
   *
   * @param slotNumber slot number up to which all nodes have applied committedRequests.
   */
  private void deleteDecisionsAcceptedByAll(int slotNumber) {

    int minSlot = Integer.MAX_VALUE;
    for (int slot : committedRequests.keySet()) {
      if (slot < minSlot) minSlot = slot;
    }
    for (int i = minSlot; i < slotNumber; i++) // < sign is very important
      committedRequests.remove(i);

  }


  /*****************BEGIN: Cooordinator-related private methods*****************/

  /**
   * This method returns the slot number for next request which coordinator will propose.
   * If the coordinator has already proposed stop command, this method returns -1,
   * and coordinator will not propose any more requests.
   */
  private int getNextProposalSlotNumber() {
    synchronized (proposalNumberLock) {
      if (stopCommandProposed) return -1;
      nextProposalSlotNumber += 1;
      return nextProposalSlotNumber - 1;
    }
  }

  /**
   * Method updates the value of variable <code>nextProposalSlotNumber</code>.
   * A new coordinator calls this method immediately after getting elected.
   * See documentation of <code>handleAdoptedNew</code> for more clarification.
   *
   * @param slotNumber set value of <code>nextProposalSlotNumber</code> to this value
   */
  private void updateNextProposalSlotNumber(int slotNumber) {
    synchronized (proposalNumberLock) {
      if (stopCommandProposed) return;
      if (slotNumber > nextProposalSlotNumber) nextProposalSlotNumber = slotNumber;
    }
  }

  /**
   * Coordinator handles a request received from one of the replicas.
   * If the node is currently coordinator, it starts the process of
   * agreement among replicas by sending accept message.
   * If node is not currently coordinator, or it has received a stop message,
   * it drops the request.
   *
   * @param p request from a replica
   * @throws JSONException
   */
  private void handleProposal(ProposalPacket p) throws JSONException {


    if (debugMode)
      GNS.getLogger().fine(paxosID + "C\t" + nodeID +
              " Coordinator handling proposal: " + p.toJSONObject().toString());

    Ballot b = null;
    try {
      coordinatorBallotLock.lock();
      if (activeCoordinator) {
        if (debugMode) GNS.getLogger().fine(paxosID + "C\t" + nodeID +
                " Active coordinator yes: ");
        p.slot = getNextProposalSlotNumber();
        if (debugMode) GNS.getLogger().fine(paxosID + "C\t" + nodeID + " Slot = " + p.slot);
        if (p.slot == -1) return;
        b = coordinatorBallot;
      } else {
        if (debugMode) GNS.getLogger().fine(paxosID + "C\t" + nodeID +
                " Active coordinator no: ");
      }
    } finally {
      coordinatorBallotLock.unlock();
    }
    if (b != null) {
      if (debugMode) GNS.getLogger().fine(paxosID + "C\t" + nodeID + "C " +
              "Coordinator starting commander at slot = " + p.slot);
      initCommander(new PValuePacket(b, p));
    }

  }

  /**
   * This method returns the maximum slot number at any replicas
   * as per this coordinator node. This is determined from the hashmap
   * <code>nodeAndSlotNumbers</code> whose key=nodeID, and value = slotNumber At that replica.
   * <p/>
   * This is called immediately after new coordinator is elected.
   * See documentation of <code>handleAdoptedNew</code> for more clarification.
   *
   * @return
   */
  private int getMaxSlotNumberAcrossNodes() {
    synchronized (nodeAndSlotNumbers) {
      int max = 0;
      for (int x : nodeAndSlotNumbers.keySet()) {
        if (debugMode)
          GNS.getLogger().fine(paxosID + "C\t" + nodeID + "C\tNode = " + x + "\tSlot = " + nodeAndSlotNumbers.get(x));
        if (nodeAndSlotNumbers.get(x) > max) max = nodeAndSlotNumbers.get(x);
      }
      return max;
    }
  }

  /**
   * Immediately after a new coordinator is elected this method is called to
   * re-propose any ongoing requests.
   * <p/>
   * Coordinator scans the list of pValues received from each replica which are
   * stored in <code>pValuesScout</code> and prepares two lists. First is the list of
   * slot for which requests that are possibly in progress ("possibly" because in some of those
   * slots request may have been been committed by previous coordinator). Second is the list of
   * slots for which no previous requests have been determined at a majority of replicas.
   * But these slot numbers are less than the maximum slot number for which a previous request
   * has been found. To fill these gaps coordinator proposes no-op requests in these slots.
   * <p/>
   * What happens if coordinator finds that a STOP request may have been proposed previously.
   * Lets say coordinator finds that a STOP may have been proposed previously in slot = x.
   * But it also finds some request that is in slot = x + 1.
   * This case can happen if the coordinator who proposed STOP request in slot = x died before
   * it was committed, and a different coordinator proposed the request in slot = x + 1, because
   * it did not learn that a STOP was proposed in slot = x.
   * Now, what should this coordinator do?
   * If coordinator proposes a STOP request in slot = x, then it should not propose any
   * request in slot = x+1. But, it is possible that STOP request may not have been committed in slot = x,
   * but the request in slot = x+1 is actually committed.
   * <p/>
   * The coordinator decides whether to propose a no-op in slot x or propose a stop in slot = x
   * based on the ballot number associated with each request.
   * <p/>
   * Case 1: If the ballot associated with request in slot = x+1 is higher than the ballot associated with
   * the request in slot = x, i.e., the coordinator who proposed request in slot = x +1 was elected
   * after the coordinator who proposed the STOP. This confirms that STOP request wasn't committed in slot = x or else
   * the coordinator who proposed the request in slot = x + 1 would never have proposed it.
   * Coordinator proposes a NOOP in slot = x.
   * <p/>
   * Case 2: If the ballot associated with request in slot = x+1 is less than the ballot associated with
   * the request in slot = x. In this case, a STOP request is proposed.
   * <p/>
   * Case 3: If the ballot associated with request in slot = x+1 is equal to the ballot associated with
   * the request in slot = x. This wont happen, because same coordinator would not propose another request
   * in a higher slot number after proposing the stop request.
   * <p/>
   * If this is still unclear, refer to the paper: Stoppable Paxos.
   * <p/>
   * http://research.microsoft.com/apps/pubs/default.aspx?id=101826
   *
   * @throws JSONException
   */
  private void handleAdoptedNew() throws JSONException {

    // NOTE: this is happening under
    if (debugMode)
      GNS.getLogger().fine(paxosID + "C\t" + nodeID + "C Ballot adopted. Handling proposals. Paxos ID = " + paxosID +
              " Nodes = " + nodeIDs);

    // propose pValue received
    // considered.
    ArrayList<Integer> pValueSlots = new ArrayList<Integer>(pValuesScout.keySet());
    Collections.sort(pValueSlots);
    int stopCommandSlot = -1;

    // this is the list of slots for which new requests will be proposed
    ArrayList<PValuePacket> selectPValues = new ArrayList<PValuePacket>();
    for (int slot : pValueSlots) {
      if (pValuesScout.get(slot).proposal.req.isStopRequest()) {
        // check if it is to be executed
        boolean checkOutput = isStopCommandToBeProposed(slot);
        if (checkOutput) {
          // propose stop command
          selectPValues.add(new PValuePacket(ballotScout, pValuesScout.get(slot).proposal));
          stopCommandSlot = slot;
          break;
        } else {
          // remove pvalue at slot, so that no-op is proposed
          pValuesScout.remove(slot);
        }
      } else { // if (!r.isDecisionComplete(proposals.get(slot)))
        if (debugMode) GNS.getLogger().fine(paxosID + "C\t" + nodeID + "C " + "PValue proposal for slot: " + slot);
        selectPValues.add(new PValuePacket(ballotScout, pValuesScout.get(slot).proposal));
      }
    }
    if (debugMode)
      GNS.getLogger().fine(paxosID + "C\t" + nodeID + "C " + "Total PValue proposals : " + selectPValues.size());


    int maxSlotNumberAtReplica = getMaxSlotNumberAcrossNodes(); // we are sure that all slots have been filled
    //  maxSlotNumberAtReplica

    if (debugMode)
      GNS.getLogger().fine(paxosID + "C\t" + nodeID + "C " + "Max slot number at replicas = " + maxSlotNumberAtReplica);

    int maxPValueSlot = -1; // this is maximum slot for which some request has been found that is in progess
    for (int x : pValuesScout.keySet()) {
      if (x > maxPValueSlot) {
        maxPValueSlot = x;
      }
    }
    if (debugMode)
      GNS.getLogger().fine(paxosID + "C\t" + nodeID + "C " + "Received pValues for slots  = " + pValuesScout.keySet() +
              " max slot = " + maxPValueSlot);

    if (stopCommandSlot != -1 && stopCommandSlot < maxPValueSlot)
      maxPValueSlot = stopCommandSlot;


    ArrayList<Integer> noopSlots = new ArrayList<Integer>();// this is the list of slots for which noops will be proposed

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
    if (debugMode) GNS.getLogger().fine(paxosID + "C\t" + nodeID + "C Next proposal slot number is: " + temp);

    // setting these variables completes the new coordinator election
    try {
      coordinatorBallotLock.lock();
      coordinatorBallot = ballotScout;
      activeCoordinator = true;
      pValuesCommander.clear();
    } finally {
      coordinatorBallotLock.unlock();
    }

    if (debugMode) GNS.getLogger().fine(paxosID + "C\t" + nodeID + "C " + " Now starting commanders for pvalues ...");
    for (PValuePacket p : selectPValues) {
      initCommander(p);
    }

    if (debugMode)
      GNS.getLogger().fine(paxosID + "C\t" + nodeID + "C " + " Now proposing no-ops for missing slots ...");
    for (int x : noopSlots) {
      proposeNoopInSlot(x, ballotScout);
    }

  }

  /**
   * On a STOP command in pValuesScout, check whether to propose it or not
   *
   * @param stopSlot the slot at which stop command is executed.
   * @return false if the stop command is not to be proposed (noop will be proposed instead),
   * true if coordinator should propose the stop command.
   */
  private boolean isStopCommandToBeProposed(int stopSlot) {
    Ballot stopBallot = pValuesScout.get(stopSlot).ballot;
    for (int x : pValuesScout.keySet()) {
      if (x > stopSlot) {
        // check whether this ballot is higher than ballot of stop slot
        Ballot xBallot = pValuesScout.get(x).ballot;
        // the ballot at this slot is higher than ballot at "stopSlot",
        // therefore "stopSlot" would not have been succesful.
        if (xBallot.compareTo(stopBallot) > 0) return false;
      }
    }
    return true;
  }

  /**
   * propose no-op command in given slot
   *
   * @param slot slot number at which no-op command is to be proposed
   * @throws JSONException
   */
  private void proposeNoopInSlot(int slot, Ballot b) throws JSONException {
    ProposalPacket proposalPacket = new ProposalPacket(slot,
            new RequestPacket(0, PaxosReplica.NO_OP, PaxosPacketType.REQUEST, false),
            PaxosPacketType.PROPOSAL, 0);
    initCommander(new PValuePacket(b, proposalPacket));
    if (debugMode) GNS.getLogger().fine(paxosID + "C\t" + nodeID + "C Proposed NO-OP in slot = " + slot);
  }

  /**
   * Send message to acceptors with the proposed value and slot number.
   *
   * @throws JSONException
   */
  private void initCommander(PValuePacket pValue) throws JSONException {
    // keep record of value
    ProposalStateAtCoordinator propState = new ProposalStateAtCoordinator(this, pValue, nodeIDs.size());

    pValuesCommander.put(pValue.proposal.slot, propState);
    paxosManager.addToActiveProposals(propState);
    if (debugMode) GNS.getLogger().fine(paxosID + "C\t" + nodeID +
            "C initialized commander values. Slot = " + pValue.proposal.slot);

    int minSlot;
    synchronized (minSlotLock) {
      minSlot = minSlotNumberAcrossNodes;
    }
    if (pValue.proposal.req.isStopRequest()) {
      synchronized (proposalNumberLock) {
        stopCommandProposed = true;
        if (debugMode) GNS.getLogger().info(paxosID + "C\t" + nodeID + "C" +
                " stop command proposed. in slot = " + pValue.proposal.slot);
      }
    }
    AcceptPacket accept = new AcceptPacket(this.nodeID, pValue,
            PaxosPacketType.ACCEPT, minSlot);


    JSONObject jsonObject = accept.toJSONObject();
    paxosManager.sendMessage(nodeIDs, jsonObject, paxosID);

  }

  /**
   * SynchronizeReplyPacket is sent by a replica to a coordinator informing its
   * <code>slotNumber</code> and if it has not received any requests.
   * <p/>
   * Coordinator keeps tracks of slotNumber at replicas for garbage collection.
   * If coordinator has any requests which replica did not receive, it resends them.
   *
   * @param replyPacket packet received by coordinator from a replica
   */
  private void handleSyncReplyPacket(SynchronizeReplyPacket replyPacket) {

    updateNodeAndSlotNumbers(replyPacket); // update

    if (replyPacket.missingSlotNumbers != null) {

      for (int x : replyPacket.missingSlotNumbers) {
        if (committedRequests.containsKey(x)) {
          RequestPacket decision = committedRequests.get(x);
          if (decision == null) continue;
          ProposalPacket packet = new ProposalPacket(x, decision, PaxosPacketType.DECISION, 0);

          sendMessage(replyPacket.nodeID, packet);
        }
      }
    }
    if (replyPacket.flag) {
      // resend committedRequests after max decision slot
      for (int x : committedRequests.keySet()) {
        if (x > replyPacket.maxDecisionSlot) {
          RequestPacket decision = committedRequests.get(x);
          if (decision == null) continue;
          ProposalPacket packet = new ProposalPacket(x, decision, PaxosPacketType.DECISION, 0);
          sendMessage(replyPacket.nodeID, packet);
        }
      }
    }
  }

  /**
   * Handle response of accept message from an acceptor.
   * <p/>
   * If reply contains a ballot number higher than
   * the coordinator's current ballot number, coordinator is pre-empted.
   * Otherwise, coordinator adds this node to the set of nodes who have accepted. Once
   * majority of nodes have accepted this message, coordinator sends commit to all replicas.
   *
   * @param accept Accept message sent by a paxos replica.
   * @throws JSONException
   */
  private void handleAcceptMessageReply(AcceptReplyPacket accept) throws JSONException {
    // resend missing requests
//        updateNodeAndSlotNumbers(accept.nodeID, accept.slotNumberAtReplica);

    int slot = accept.slotNumber;
    Ballot b = accept.ballot;
    int senderNode = accept.nodeID;
    if (debugMode) GNS.getLogger().fine(paxosID + "C\t" + nodeID + "C Accept-Reply\tsender\t" + accept.nodeID
            + " slot\t" + slot);//+  "  accept = " + accept.toJSONObject().toString());
    ProposalStateAtCoordinator stateAtCoordinator = pValuesCommander.get(slot);

    if (stateAtCoordinator == null) {
      if (debugMode) GNS.getLogger().fine(paxosID + "C\t" + nodeID + "C " +
              "Commander not found for slot =  " + slot + ". Either decision complete or ballot preempted.");
      return;
    }

    if (b.compareTo(stateAtCoordinator.pValuePacket.ballot) <= 0) {
      // case: replica accepted proposed value
//                if (debugMode) GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C ballot OK. ");
      stateAtCoordinator.addNode(senderNode);
//                if (debugMode) GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C commander accept count: " +
//                        stateAtCoordinator.getNodesResponded() + " nodes "+ nodeIDs.size());
      sendDecisionForSlot(slot, stateAtCoordinator);
    } else if (b.compareTo(stateAtCoordinator.pValuePacket.ballot) > 0) {
      // case: ballot preempted
      stateAtCoordinator = pValuesCommander.remove(slot);

      if (stateAtCoordinator != null) {
        paxosManager.removeFromActiveProposals(stateAtCoordinator);
        if (debugMode)
          GNS.getLogger().fine(paxosID + "C\t" + nodeID + "C " + "higher ballot recvd. current ballot preempted.");
        try {
          acceptorLock.lock();
          try {
            coordinatorBallotLock.lock();
            // update variables to indicate I am no longer coordinator
            if (coordinatorBallot.compareTo(stateAtCoordinator.pValuePacket.ballot) == 0 && activeCoordinator) {
              activeCoordinator = false;
              coordinatorBallot = b;
              pValuesCommander.clear();
            }
          } finally {
            coordinatorBallotLock.unlock();
          }
          // also update acceptorBallot
          if (stateAtCoordinator.pValuePacket.ballot.compareTo(acceptorBallot) > 0) {
            acceptorBallot = new Ballot(b.ballotNumber, b.coordinatorID);
          }
        } finally {
          acceptorLock.unlock();
        }
        // if I should be next coordinator based on set of nodes active, I will try to get reelected as coordinator.
        if (getNextCoordinatorReplica() == nodeID) initScout();
      }
    }

  }

  /**
   * This method check if majority of replicas have replied to accept message for a given slot.
   * if yes, send commit to all replicas.
   *
   * @param slot               slot number for which majority is to be checked
   * @param stateAtCoordinator variable containing list of nodes already responeded.
   */
  private void sendDecisionForSlot(int slot, ProposalStateAtCoordinator stateAtCoordinator) {

    if (!stateAtCoordinator.isMajorityReached()) {
      return;
    }


    // remove object from pValuesCommander
    stateAtCoordinator = pValuesCommander.remove(slot);
    paxosManager.removeFromActiveProposals(stateAtCoordinator);
    // if object deleted return
    if (stateAtCoordinator == null) return;

    stateAtCoordinator.pValuePacket.proposal.makeDecisionPacket();

    int minSlot;
    synchronized (minSlotLock) {
      minSlot = minSlotNumberAcrossNodes;
    }

    try {
      stateAtCoordinator.pValuePacket.proposal.gcSlot = minSlot;
      JSONObject jsonObject = stateAtCoordinator.pValuePacket.proposal.toJSONObject();
      jsonObject.put(PaxosManager.PAXOS_ID, paxosID);
      paxosManager.sendMessage(nodeIDs, jsonObject, paxosID);

    } catch (JSONException e) {
      e.printStackTrace();
    }
  }

  /**
   * Send prepare packet to get new ballot accepted.
   */
  private void initScout() {

    synchronized (proposalNumberLock) {
      if (stopCommandProposed) return;
    }

    try {
      scoutLock.lock();
      try {
        acceptorLock.lock();
//            synchronized (acceptorBallot) {
        try {
          coordinatorBallotLock.lock();

          // scout will now choose a ballot
          if (ballotScout != null && ballotScout.compareTo(acceptorBallot) > 0) {
            ballotScout = new Ballot(ballotScout.ballotNumber + 1, nodeID);
          } else {
            ballotScout = new Ballot(acceptorBallot.ballotNumber + 1, nodeID);
          }
        } finally {
          coordinatorBallotLock.unlock();
        }

      } finally {
        acceptorLock.unlock();
      }

      GNS.getLogger().severe(paxosID + "C\t" + nodeID + "C Coordinator proposing new ballot: " + ballotScout);

      waitForScout = new ArrayList<Integer>();
      pValuesScout = new HashMap<Integer, PValuePacket>();
    } finally {
      scoutLock.unlock();
    }

    // create prepare packet
    PreparePacket prepare = new PreparePacket(nodeID, 0, ballotScout, PaxosPacketType.PREPARE);

    for (Integer i : nodeIDs) {
      prepare.receiverID = i;
//            if (debugMode) GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C sending prepare msg to " +i );
      sendMessage(i, prepare);
    }

    // create a task to retry sending the prepare message until majority of replicas respond.
    CheckPrepareMessageTask task = new CheckPrepareMessageTask(this, prepare, ballotScout);

    paxosManager.executorService.scheduleAtFixedRate(task, PaxosManager.RESEND_PENDING_MSG_INTERVAL_MILLIS,
            PaxosManager.RESEND_PENDING_MSG_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);

  }

  /**
   * Returns true if there is a need to resend the prepare message again, otherwise false.
   *
   * @param proposedBallot ballot that coordinator is proposing
   * @param prepare        prepare packet sent to replicas
   * @return true if there is a need to resend the prepare message again, otherwise false.
   */
  public boolean resendPrepare(Ballot proposedBallot, PreparePacket prepare) {
    try {
      scoutLock.lock();
      if (proposedBallot.equals(ballotScout) && waitForScout != null) {
        // what if majority have failed?
        int nodesUp = 0;
        for (int x : nodeIDs)
          if (paxosManager.isNodeUp(x)) nodesUp++;
        if (nodesUp * 2 < nodeIDs.size()) return false; // more than half node down, give up resending prepare

        boolean resend = false;
        for (int x : nodeIDs) {
          if (!waitForScout.contains(x) && paxosManager.isNodeUp(x)) {
            sendMessage(x, prepare); // send message to the node if it is up and has not responded.
            resend = true;
            if (debugMode)
              GNS.getLogger().fine(paxosID + "\t" + nodeID + "C\t Ballot = " + proposedBallot + " resend to node " + x);
          }
        }
        return resend;
      } else {
        if (debugMode)
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
   * committedRequests until <code>slotNumber - 1</code>. Once all replicas have executed all committedRequests until a
   * slot number, coordinator informs replicas to delete state before that slot.
   *
   * @param nodeID     ID of node whose information is updated
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
   * all committedRequests until a slot number, coordinator informs replicas to delete state before that slot.
   *
   * @param replyPacket packet received by coordinator from a replica
   */
  private void updateNodeAndSlotNumbers(SynchronizeReplyPacket replyPacket) {

    int minSlot = Integer.MAX_VALUE;
    if (replyPacket.missingSlotNumbers != null) {
      for (int x : replyPacket.missingSlotNumbers) {
        if (minSlot > x) minSlot = x;
      }
    } else {
      minSlot = replyPacket.maxDecisionSlot;
      minSlot += 1;
    }

    if (minSlot == Integer.MAX_VALUE) return;

    int node = replyPacket.nodeID;

    if (!nodeAndSlotNumbers.containsKey(node) || minSlot > nodeAndSlotNumbers.get(node)) {
      if (debugMode) GNS.getLogger().finer(paxosID + "C\t" + node + "C node = " + node
              + " Update: slot number = " + minSlot);
      nodeAndSlotNumbers.put(node, minSlot);
    }
    synchronized (minSlotLock) {
      minSlotNumberAcrossNodes = getMinSlotNumberAcrossNodes();
    }

  }

  /**
   * Handle reply from a replica for a prepare message.
   * The replies also contain a list of pvalues which are the requests currently in progress.
   * <p/>
   * If a replica replies with a ballot number greater than this coordinator's
   * <code>ballotScout</code>, it means this coordinator's ballot is rejected and replica
   * cannot become coordinator.
   *
   * @throws JSONException
   */
  private void handlePrepareMessageReply(PreparePacket prepare) throws JSONException {


    updateNodeAndSlotNumbers(prepare.receiverID, prepare.slotNumber); // keep track of slot number at each replica.
    boolean tryReelect = false;
    try {
      scoutLock.lock();

      if (waitForScout == null) {
        if (debugMode) GNS.getLogger().fine(paxosID + "C\t" + nodeID + "C not getting ballot accepted now.");
        return;
      }
      if (debugMode) GNS.getLogger().finer(paxosID + "C\t" + nodeID
              + "C received Prepare-Reply Msg " + prepare.toJSONObject().toString());
      if (waitForScout.contains(prepare.receiverID)) {
        return;
      }

      if (prepare.ballot.compareTo(ballotScout) < 0) {
//                if (debugMode) GNS.getLogger().fine(paxosID + "C\t" +nodeID
//                        + "C received response for a earlier scout. Ballot scout = " + ballotScout
//                        + " Message ballot = " + prepare.ballot);
        return;
      } else if (prepare.ballot.compareTo(ballotScout) > 0) {
        if (debugMode) GNS.getLogger().fine(paxosID + "C\t" + nodeID + "C Ballot pre-empted.");
        waitForScout = null;
        tryReelect = true;
      } else {
        if (debugMode) GNS.getLogger().finer(paxosID + "C\t" + nodeID + "C Ballot accepted " +
                ballotScout + " by node " + prepare.receiverID);
        for (PValuePacket pval : prepare.accepted.values()) {
          int slot = pval.proposal.slot;
          if (!pValuesScout.containsKey(slot) ||
                  prepare.ballot.compareTo(pValuesScout.get(slot).ballot) > 0) {
            pValuesScout.put(slot, pval);

          }
        }

        waitForScout.add(prepare.receiverID);
        if (waitForScout.size() > nodeIDs.size() / 2) { // activeCoordinator == false &&
          // case: ballot accepted by majority, cooridinator elected
          if (debugMode) GNS.getLogger().fine(paxosID + "C\t" + nodeID + "C " +
                  "Scout received majority votes for ballot. " + ballotScout + "Votes = "
                  + waitForScout.size() + " Nodes =  " + nodeIDs.size());
          waitForScout = null;
          handleAdoptedNew();
        }
      }
    } finally {
      scoutLock.unlock();
    }
    // if I should be the next coordinator based of set of active nodes, I will try to get re-elected.
    if (tryReelect && getNextCoordinatorReplica() == nodeID) initScout();
  }

  /**
   * Check whether coordinator is UP. If coordinator has failed, test where I am the next node to propose
   * a new ballot.
   */
  public void checkCoordinatorFailure() {
    int coordinatorID = -1;
    try {
      acceptorLock.lock();

      if (acceptorBallot != null) {
        coordinatorID = acceptorBallot.coordinatorID;
      }
    } finally {
      acceptorLock.unlock();
    }

    if (!paxosManager.isNodeUp(coordinatorID) && getNextCoordinatorReplica() == nodeID) {
      GNS.getLogger().severe(paxosID + "C\t" + nodeID + "C coordinator failed. " +
              coordinatorID);
      initScout();
    }
  }

  /**
   * Message reporting node failure.
   * We check if the failed node is current coordinator. if yes, and if I am next node who should
   * become the coordinator I will try to get elected.
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

    if (debugMode)
      GNS.getLogger().fine(paxosID + "C\t" + nodeID + " Node failed:" + packet.responderNodeID);

    int coordinatorID = -1;
    try {
      acceptorLock.lock();
      if (acceptorBallot != null) {
        coordinatorID = acceptorBallot.coordinatorID;
      }
    } finally {
      acceptorLock.unlock();
    }
    if (packet.responderNodeID == coordinatorID // current coordinator has failed.
            && getNextCoordinatorReplica() == nodeID) { // I am next coordinator

      GNS.getLogger().warning(paxosID + "C\t" + nodeID + " Coordinator failed\t" + packet.responderNodeID +
              " Propose new ballot.");
      GNS.getLogger().severe(paxosID + "C\t" + nodeID + "C coordinator has failed " + coordinatorID);
      if (debugMode)
        GNS.getLogger().fine(paxosID + "C\t" + nodeID + " current ballot coordinator failed " + coordinatorID);
      // I should try to get a new ballot accepted.
      initScout();
    }
  }

  /**
   * @return Minimum slot number in the list 'nodeAndSlotNumbers' across all replicas.
   * We can safely garbage collect state for slots at less than this value.
   */
  private int getMinSlotNumberAcrossNodes() {
    if (nodeAndSlotNumbers.size() < nodeIDs.size()) return -1;
    int minSlot = -1;
    for (int nodeID : nodeAndSlotNumbers.keySet()) {
      if (minSlot == -1 || minSlot > nodeAndSlotNumbers.get(nodeID)) {
        minSlot = nodeAndSlotNumbers.get(nodeID);
      }
    }
    return minSlot;
  }

  /*****************END: Cooordinator-related private methods*****************/

}

/**
 * Variable stores all state for a request proposed by coordinator.
 * We track the send time of request, and the set of nodes who have responded.
 */
class ProposalStateAtCoordinator implements Comparable, Serializable {

  public PaxosReplicaInterface paxosReplica;
  private long acceptSentTime = -1;
  PValuePacket pValuePacket;
  private ConcurrentHashMap<Integer, Integer> nodes;
  int numReplicas;

  private int resendCount = 0;

  public ProposalStateAtCoordinator(PaxosReplicaInterface paxosReplica, PValuePacket pValuePacket, int numReplicas) {
    this.paxosReplica = paxosReplica;
    this.pValuePacket = pValuePacket;
    this.nodes = new ConcurrentHashMap<Integer, Integer>();
    this.acceptSentTime = System.currentTimeMillis();
    this.numReplicas = numReplicas;
  }

  public void addNode(int node) {
    nodes.put(node, node);
  }

  private boolean majority = false;

  public synchronized boolean isMajorityReached() {

    if (majority) return false; // we return true only once, to avoid sending duplicate commit messages.

    if (nodes.size() * 2 > numReplicas) {
      majority = true;
      return true;
    }
    return false;
  }

  public boolean hasNode(int node) {
    return nodes.containsKey(node);
  }

  public long getTimeSinceAccept() {
    if (acceptSentTime == -1) acceptSentTime = System.currentTimeMillis();
    return System.currentTimeMillis() - acceptSentTime;
  }

  @Override
  public int compareTo(Object o) {
    ProposalStateAtCoordinator p = (ProposalStateAtCoordinator) o;
    if (acceptSentTime < p.acceptSentTime) return -1;
    else if (acceptSentTime > p.acceptSentTime) return 1;
    int x = paxosReplica.getPaxosID().compareTo(p.paxosReplica.getPaxosID());
    if (x != 0) return x;
    if (pValuePacket.proposal.slot < p.pValuePacket.proposal.slot) return -1;
    if (pValuePacket.proposal.slot > p.pValuePacket.proposal.slot) return 1;
    return 0;
  }


  public int getResendCount() {
    return resendCount;
  }

  public void increaseResendCount() {
    resendCount += 1;
  }

  // main method to test serialization/deserialization performance of paxos replica
  public static void main(String[] args) throws IOException, ClassNotFoundException {
    String name = "abcd";
    HashSet<Integer> x = new HashSet<Integer>();
    x.add(0);
    x.add(1);
    x.add(2);
    PaxosReplica replica = new PaxosReplica(name, 0, x, null);
//    FileOutputStream fileOut = new FileOutputStream("/tmp/employee.ser");

    int repeat = 100000;
    long t0 = System.currentTimeMillis();
    for (int i = 0; i < repeat; i++) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream(10000);
      ObjectOutputStream out = new ObjectOutputStream(baos);
      out.writeObject(replica);
      out.close();
    }
    long t1 = System.currentTimeMillis();
    double avg = (t1 - t0) * 1.0 / repeat;
    System.out.println("average serialization latency: " + avg + " ms");

    ByteArrayOutputStream baos = new ByteArrayOutputStream(10000);
    ObjectOutputStream out = new ObjectOutputStream(baos);
    out.writeObject(replica);
    out.close();
    byte[] byteArray = baos.toByteArray();
    System.out.println("byte array size: " + byteArray.length + " bytes");

    t0 = System.currentTimeMillis();
    for (int i = 0; i < repeat; i++) {
      ByteArrayInputStream bais = new ByteArrayInputStream(byteArray);
      ObjectInputStream in = new ObjectInputStream(bais);
      in.readObject();
      in.close();
    }
    t1 = System.currentTimeMillis();
    avg = (t1 - t0) * 1.0 / repeat;
    System.out.println("average de-serialization latency: " + avg + " ms");
  }
}

/**
 * This class periodically resends prepare message if it is not accepted by majority.
 */
class CheckPrepareMessageTask extends TimerTask {

  private static final int NUM_RETRY = 10;

  int count = 0;
  PaxosReplicaInterface replica;
  PreparePacket preparePacket;
  Ballot proposedBallot;

  public CheckPrepareMessageTask(PaxosReplicaInterface replica, PreparePacket preparePacket, Ballot proposedBallot) {
    this.replica = replica;
    this.preparePacket = preparePacket;
    this.proposedBallot = proposedBallot;
  }

  @Override
  public void run() {
    try {
      if (replica.isStopped()) throw new CancelExecutorTaskException();
      boolean sendAgain = replica.resendPrepare(proposedBallot, preparePacket);
      if (!sendAgain) throw new CancelExecutorTaskException();
      if (count == NUM_RETRY) {
        GNS.getLogger().severe("Prepare message max retries reached. Aborting. PaxosID " + replica.getPaxosID()
                + "\tprepare " + preparePacket + "\tnodeIDs " + replica.getNodeIDs() + "\t");
      }
    } catch (Exception e) {
      if (e.getClass().equals(CancelExecutorTaskException.class)) {
        throw new RuntimeException(); // throwing RuntimeException here to finally kill the task
      }
      GNS.getLogger().severe("Exception in CheckPrepareMessageTask: " + e.getMessage());
      e.printStackTrace();
    }
  }
}

/**
 * If a replica finds that current coordinator has failed, this task tries to find the new coordinator by
 * contacting other replicas.
 * <p/>
 * This task is created after a replica receives a request.
 */
class UpdateBallotTask extends TimerTask {

  /**
   * Paxos replica that created this task.
   */
  PaxosReplicaInterface replica;

  /**
   * Value of <code>acceptorBallot</code> at the time this task was created.
   * This task is complete once the paxos replica finds a new <code>acceptorBallot</code>.
   */
  Ballot ballot;

  /**
   * number of attempts.
   */
  int count = 0;

  /**
   * Nodes which I have contacted so far.
   */
  ArrayList<Integer> nodes;

  /**
   * The json message to send to that replica asking for a new coordinator.
   */
  JSONObject json;


  PaxosManager paxosManager;

  public UpdateBallotTask(PaxosManager paxosManager, PaxosReplicaInterface replica, Ballot ballot, JSONObject json) {
    this.replica = replica;
    this.ballot = ballot;
    String paxosID1 = paxosManager.getPaxosKeyFromPaxosID(replica.getPaxosID()); //paxosID.split("-")[0];
    Random r = new Random(paxosID1.hashCode());
    nodes = new ArrayList<Integer>(replica.getNodeIDs());
    Collections.sort(nodes);
    Collections.shuffle(nodes, r);
    this.json = json;
    this.paxosManager = paxosManager;
  }

  @Override
  public void run() {
    if (count == nodes.size()) {
      throw new RuntimeException();
    }
    PaxosReplicaInterface pr = paxosManager.paxosInstances.get(paxosManager.getPaxosKeyFromPaxosID(replica.getPaxosID()));
    if (pr == null || !pr.getPaxosID().equals(replica.getPaxosID())) {
      throw new RuntimeException();
    }
    if (pr.isAcceptorBallotUpdated(ballot)) {
      throw new RuntimeException();
    }
    int node = -1;
    while (node == -1) {
      if (count < nodes.size() && paxosManager.isNodeUp(nodes.get(count))) {
        node = nodes.get(count);
      }
      count += 1;
    }
    if (node > 0) {
      paxosManager.sendMessage(node, json, replica.getPaxosID());
    }
  }
}