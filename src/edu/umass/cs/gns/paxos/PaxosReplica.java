package edu.umass.cs.gns.paxos;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartNameServer;
import edu.umass.cs.gns.nameserver.GenerateSyntheticRecordTable;
import edu.umass.cs.gns.nameserver.NameServer;
import edu.umass.cs.gns.packet.paxospacket.*;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
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
public class PaxosReplica {

  public static String Version = "$Revision$";
  
  /**
   * Special no-op command.
   */
  static final String NO_OP = "NO_OP";

  /**
   * Special command to stop a paxos instance.
   */
  static final String STOP = "STOP";

  // Data structures maintained by Paxos

  /**
   * ID of the paxos instance this replica belongs to.
   */
  private String paxosID;

  /**
   * ID of this replica.
   */
  private static int nodeID = NameServer.nodeID;

  /**
   * IDs of nodes that belong to this paxos group.
   */
  private Set<Integer> nodeIDs;

//    Integer[] nodeIDArray;

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
  private int garbageCollectionSlot = 0;

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

//  private RequestPacket lastRequest = null;


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
  private ConcurrentHashMap<Integer,Integer> nodeAndSlotNumbers = new ConcurrentHashMap<Integer, Integer>();

  // PAXOS-STOP
  /**
   * True if stop command is proposed by coordinator. Once stop command is decided in a slot,
   * the coordinator does not propose any commands in any higher numbered slots.
   */
  private boolean stopCommandProposed = false;

  /**
   * Timeout value after which accept messages for a slot are resent to nodes who have not yet responded
   */
  private static long RESEND_TIMEOUT = 1000;

  /**
   * Time when coordinator completed the previous decision
   */
  static long lastDecisionTime = -1;

  /**
   * Object used to synchronize access to {@code lastDecisionTime}
   */
  private static final ReentrantLock lastDecisionTimeLock = new ReentrantLock();

  /**
   * Used for testing. When did this node last tried to take over as coordinator.
   */
//  private long lastCoordinatorTakeoverTime = -1;

  /**
   *  Used for testing. Object used to synchronize access to {@code lastCoordinatorTakeoverTime}
   */
//  private final ReentrantLock lastCoordinatorTakeoverTimeLock = new ReentrantLock();


//  private int nodeIDcount = 0;

//  HashSet<Integer> selectNodes = new HashSet<Integer>();

  /**
   * Constructor.
   * @param paxosID ID of this paxos group
   * @param ID  ID of this node
   * @param nodeIDs set of IDs of nodes in this paxos group
   */
  public PaxosReplica(String paxosID, int ID, Set<Integer> nodeIDs) {
//    System.out.println("In constructor...");
    this.paxosID = paxosID;
//    this.nodeID = ID;
    this.nodeIDs = nodeIDs;
//    this.nodeIDcount = nodeIDs.size();

//    updateSelectNodes();
    acceptorBallot = new Ballot(0, getDefaultCoordinatorReplica());
//        PaxosLogger.logCurrentBallot(paxosID, acceptorBallot);
  }


  public PaxosReplica(String paxosID, int ID, Set<Integer> nodeIDs, boolean isStopped, RequestPacket lastRequest) {

    this.paxosID = paxosID;
//    this.nodeID = ID;
    this.nodeIDs = nodeIDs;
    this.isStopped = isStopped;
//    this.lastRequest = lastRequest;
  }

//  private void updateSelectNodes() {
//    Integer[] nodeIDArray = nodeIDs.toArray(new Integer[0]);
//    selectNodes = new HashSet<Integer>();
//    selectNodes.add(nodeID);
//    while(selectNodes.size() *2 <= nodeIDcount) {
//      int x  = r.nextInt(nodeIDcount);
//      if (FailureDetection.isNodeUp(nodeIDArray[x])) selectNodes.add(nodeIDArray[x]);
//    }
//  }

  /**
   * Handle an incoming message as per message type.
   * @param json
   */
  public void handleIncomingMessage(JSONObject json, int packetType) {
    try {
      if (isStopped) {
        GNS.getLogger().warning(paxosID +"\t Paxos instance = " + paxosID + " is stopped; message dropped.");
        return;
      }
//            int incomingPacketType = json.getInt(PaxosPacketType.ptype);
      // client --> replica
      switch (packetType) {
        case PaxosPacketType.REQUEST:
//                    RequestPacket req = new RequestPacket(json);
//          GNS.getLogger().info(paxosID +"\t Request = " + json);
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
          handleDecision(proposal, json);
          break;
        // coordinator --> replica
        case PaxosPacketType.PREPARE:
          PreparePacket prepare = new PreparePacket(json);
          handlePrepare(json, prepare);
          break;
        // replica --> coordinator
        case PaxosPacketType.PREPARE_REPLY:
          prepare = new PreparePacket(json);
          handlePrepareMessageReply(prepare);
          break;
        // coordinator --> replica
        case PaxosPacketType.ACCEPT:
          AcceptPacket accept = new AcceptPacket(json);
          handleAccept(json, accept);
          break;
        // replica --> coordinator
        case PaxosPacketType.ACCEPT_REPLY:
          AcceptReplyPacket acceptReply = new AcceptReplyPacket(json);
          handleAcceptMessageReply(acceptReply);
          break;
        // replica --> replica
        case PaxosPacketType.SEND_STATE:
        case PaxosPacketType.SEND_STATE_NO_RESPONSE:
          SendCurrentStatePacket sendState = new SendCurrentStatePacket(json);
          handleSendState(sendState);
          break;

        // local failure detector --> replica
        case PaxosPacketType.NODE_STATUS:
          //				GNRS.getLogger().fine("XXXRecvd node status msg");
          FailureDetectionPacket fdPacket = new FailureDetectionPacket(json);
          handleNodeStatusUpdate(fdPacket);
          break;
        //				GNRS.getLogger().fine(ID + " received failure detection packet.
        // 				Message: " + json.toString());
        //				coordinator.handleAcceptMessage(fdPacket);
        case PaxosPacketType.SYNC_REQUEST:
          handleSyncRequest(new SynchronizePacket(json));
          break;
        case PaxosPacketType.SYNC_REPLY:
          handleSyncReplyPacket(new SynchronizeReplyPacket(json));
          break;
      }
//            else if (incomingPacketType == PaxosPacketType.REQUEST_STATE) {
//                synchronize with other replicas
//                sendCurrentStateToAllNodes();
//                requestStateTaskScheduled = false;
//            }

    } catch (JSONException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

//        if (takeOverAsCoordinator()) {
//            GNS.getLogger().fine(paxosID + "\t" + " Taking over as coordinator. msg count = " + System.currentTimeMillis());
//            initScout();
//        }

  }

  /**
   * Send message p to replica {@code destID}.
   * @throws JSONException
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
    }
  }


  /********************* START: Methods for replica recovery from paxos logs ********************************/

  /**
   * recover current ballot number from paxos logs
   * @param b
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
   * @param slotNumber
   */
  public void recoverSlotNumber(int slotNumber) {
    if (slotNumber > this.slotNumber) {
      // update garbage collection slot
      GNS.getLogger().fine(paxosID + "\t" + nodeID +
              " Paxos Recovery: slotNumber updated to " + slotNumber);
      this.slotNumber = slotNumber;
    }
  }

  /**
   * recover garbage collection slot number from paxos logs
   * @param garbageCollectionSlot
   */
  public void recoverGarbageCollectionSlot(int garbageCollectionSlot) {
    if (garbageCollectionSlot > this.garbageCollectionSlot) {
      GNS.getLogger().fine(paxosID + "\t" + nodeID +
              " Paxos Recovery: garbageCollectionSlot updated to " + garbageCollectionSlot);
      // update garbage collection slot
      this.garbageCollectionSlot = garbageCollectionSlot;
      // delete useless state
      deletePValuesDecided(garbageCollectionSlot);
      deleteDecisionsAcceptedByAll(garbageCollectionSlot);
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




  public void recoverPrepare(PreparePacket preparePacket) {
    if (preparePacket.ballot.compareTo(acceptorBallot) > 0) {
      acceptorBallot = preparePacket.ballot;
    }
  }

  public void executeRecoveredDecisions() {
    try{
      handleDecision(null, null);
    }catch (JSONException e) {
      e.printStackTrace();
    }
  }
  /**
   * Used only for testing!!
   * @return
   */
  public Ballot getAcceptorBallot() {
    return acceptorBallot;
  }

  /**
   * Used only for testing!!
   * @return
   */
  public int getSlotNumber() {
    return slotNumber;
  }

  /**
   * Used only for testing!!
   * @return
   */
  public int getGarbageCollectionSlot() {
    return garbageCollectionSlot;
  }

  /**
   * Used only for testing!!
   * @return
   */
  public ConcurrentHashMap<Integer, PValuePacket> getPValuesAccepted() {
    return pValuesAccepted;
  }

  /**
   * Used only for testing!!
   * @return
   */
  public ConcurrentHashMap<Integer, RequestPacket> getDecisions() {
    return decisions;
  }

  /********************* END: Methods for replica recovery from paxos logs ********************************/












  boolean replicaStarted = false;

  private final ReentrantLock startLock = new ReentrantLock();

  public void startReplicaWithPreelectedCoordinator() {
    synchronized (startLock) {
      if (replicaStarted) return;
      replicaStarted = true;
    }
    startPreelectedCoordinator();
  }

  public  void startReplica(long initScoutDelay) {
    synchronized (startLock) {
      if (replicaStarted) return;
      replicaStarted = true;
    }
    startCoordinator(initScoutDelay);
    // send state to all nodes.

//    sendCurrentStateToAllNodes();

//        writeCurrentState();
  }


  private int getDefaultCoordinatorReplica() {

//    int nodeProduct = 1;
//    for (int x: nodeIDs) {
//      nodeProduct =  nodeProduct*x;
//    }
    String paxosID1 = paxosID.split("-")[0];
    Random r = new Random(paxosID1.hashCode());
    ArrayList<Integer> x1  = new ArrayList<Integer>(nodeIDs);
    Collections.sort(x1);
    Collections.shuffle(x1, r);
    for (int x: x1) {
      if (PaxosManager.isNodeUp(x)) return x;
    }
    return  x1.get(0);
//    return  x1.get(count);
  }

//
  private static int getDefaultCoordinatorReplica(String paxosID, Set<Integer> nodeIDs) {
    //    int nodeProduct = 1;
    //    for (int x: nodeIDs) {
    //      nodeProduct =  nodeProduct*x;
    //    }
    String paxosID1 = paxosID.split("-")[0];
    Random r = new Random(paxosID1.hashCode());
    ArrayList<Integer> x1  = new ArrayList<Integer>(nodeIDs);
    Collections.sort(x1);
    Collections.shuffle(x1, r);
    //for (int x: x1) {
    //    if (PaxosManager.isNodeUp(x)) return x;
    //}
    return  x1.get(0);
    //    return  x1.get(count);
  }


  public static void main (String[] args) throws IOException {

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
      System.out.println(name + "\t" + defaultCoordinator);
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
  }

  /**
   * Used for testing. If true, this node will attempt to take over as coordinator.
   * @return true if node should take over as coordinator, false otherwise
   */
//  private boolean takeOverAsCoordinator() {
//    // the node 0 will take over as coordinator
////        if(nodeID != 0) return false;
//    synchronized (lastCoordinatorTakeoverTimeLock) {
////            if (lastCoordinatorTakeoverTime > 0) return false;
//      if (lastCoordinatorTakeoverTime == -1) {
//        lastCoordinatorTakeoverTime = System.currentTimeMillis();
//      }
//      // take over as coordinator after every 5 sec
//      if (System.currentTimeMillis() - lastCoordinatorTakeoverTime >= 5000) {
//        lastCoordinatorTakeoverTime = System.currentTimeMillis();
//        return  true;
//      }
//      return false;
//    }
//  }

  /**
   * Deletes state on disk when this paxos instance is being stopped.
   */
  public  void deleteState() {

  }

  /**
   * When paxos replica is starting/restarting, send exchange state with all paxos replicas.
   */
  public  void sendCurrentStateToAllNodes() {
    for(int nodeID: nodeIDs) {
      if (nodeID == this.nodeID) continue;
      // send empty json object
      sendCurrentState(nodeID, PaxosPacketType.SEND_STATE, new JSONObject());
    }
  }

  /**
   * send state to {@code receiverNode} when this node is starting/restarting
   * @param receiverNode
   * @param packetType
   * @param decisions
   */
  private void sendCurrentState(int receiverNode, int packetType, JSONObject decisions) {
    Ballot currentBallot = null;
    try { acceptorLock.lock();
      if (acceptorBallot != null)
        currentBallot = new Ballot(acceptorBallot.getBallotNumber(), acceptorBallot.getCoordinatorID());
    }finally {
      acceptorLock.unlock();
    }

    // if the coordinator is doing a prepare phase, and the prepare is still in progress.
    // send the prepare message as well.
    Ballot prepareBallot = resendPrepareMessage(receiverNode);

    int slot;
    synchronized (slotNumberLock) {
      slot = slotNumber;
    }
    SendCurrentStatePacket sendCurrentState = new SendCurrentStatePacket(nodeID,
            currentBallot, prepareBallot, packetType, slot, decisions);

    sendMessage(receiverNode, sendCurrentState);
    GNS.getLogger().fine(paxosID + "\t" +nodeID + " Sent current state to node " + receiverNode);
    GNS.getLogger().fine(paxosID + "\t" +nodeID + " Send state " + sendCurrentState.toString());
  }

  /**
   * When state is received from other nodes, reply by sending your own state.
   * @param sendState
   * @throws JSONException
   */
  private  void handleSendState(SendCurrentStatePacket sendState) throws JSONException {
    GNS.getLogger().fine(paxosID + "\t" +nodeID + " Recvd current state from " + sendState.sendingNodeID + " state: " + sendState.toString());
    if (sendState.prepareBallot != null) {
      PreparePacket p = new PreparePacket(sendState.sendingNodeID, nodeID,
              sendState.prepareBallot, PaxosPacketType.PREPARE);
      handlePrepare(p.toJSONObject(),p);
    }

    if (sendState.currentBallot != null) {
//            PaxosLogger.logCurrentBallot(paxosID, sendState.currentBallot);
      synchronized (acceptorBallot) {
        if (acceptorBallot == null || acceptorBallot.compareTo(sendState.currentBallot) < 0) {
          acceptorBallot = sendState.currentBallot;


        }

      }

    }


//        if (sendState.state != null && sendState.slotNumber > slotNumber) {
//            slotNumber = sendState.slotNumber;
//            PaxosManager.updatePaxosState(paxosID, sendState.state);
//        }

    parseDecisionsReceived(sendState.decisions);

    // send your own state, without waiting for response.
    if (sendState.packetType == PaxosPacketType.SEND_STATE) {
      sendCurrentState(sendState.sendingNodeID, PaxosPacketType.SEND_STATE_NO_RESPONSE,
              getDecisionsAfterSlotNumber(sendState.slotNumber));
    }

  }

  /**
   * get decisions after slotNumber in  {@code decisions}.
   * A single json object with all decisions is returned.
   * @param slotNumber
   * @return a json object with all decisions
   */
  private JSONObject getDecisionsAfterSlotNumber(int slotNumber) {
    // For slots greater than or equal to the slot number, get list of decisions
    GNS.getLogger().fine(paxosID + "\t" +nodeID + " Send Decisions slot number " + slotNumber);

    JSONObject json = new JSONObject();

    for (int x: decisions.keySet()) {
      GNS.getLogger().fine(paxosID + "\t" +nodeID + " Decisions for slot "+ x);
      if (x >= slotNumber) {
        GNS.getLogger().fine(paxosID + "\t" +nodeID + " INCLUDED");
        try
        {
          json.put(Integer.toString(x), decisions.get(x).toJSONObject().toString());
//					GNRS.getLogger().fine(json.toString());
        } catch (JSONException e)
        {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    }
    GNS.getLogger().fine(paxosID + "\t" + nodeID + " JSON Object created for sending " + json);
    return json;
  }

  /**
   * convert json object {@code json} to set of decisions
   * @param json
   * @throws JSONException
   */
  private void parseDecisionsReceived(JSONObject json) throws JSONException {
    GNS.getLogger().fine(paxosID + "\t" +json.toString());
    // iterate over each json item: json key = slot, json value = request packet
    for (Iterator i = json.keys(); i.hasNext();) {
      //
      String x = (String) i.next();
      String decision = json.getString(x);

      int slot = Integer.parseInt(x);
      RequestPacket req = new RequestPacket(new JSONObject(decision));

      // if we have the decision for a slot, continue
      if (!decisions.containsKey(slot)){
        // else, put the decision in the list
        decisions.put(slot, req);
      }
    }

    handleDecision(null, null);
  }

  /**
   * Check if a given node is in this Paxos instance.
   * @param nodeID
   * @return true if node belongs to this paxos instance.
   */
  public  boolean isNodeInPaxosInstance(int nodeID) {
    return nodeIDs.contains(nodeID);
  }

  /**
   * is this paxos instance stopped?
   * @return
   */
  public boolean isStopped() {
    synchronized (stopLock) {
      return isStopped;
    }
  }

  /**
   *
   * @return
   */
//  public RequestPacket getLastRequest() {
//    return lastRequest;
//  }

  /**
   *
   * @return
   */
  public Set<Integer> getNodeIDs() {
    return nodeIDs;
  }


  StatePacket getState() {

    try{
      acceptorLock.lock();
      synchronized (slotNumberLock) {
        String dbState = PaxosManager.clientRequestHandler.getState(paxosID);
        if (dbState == null) {
          GNS.getLogger().severe(paxosID + "\t" + nodeID + "\tError Exception Paxos state not logged because database state is null.");
          return null;
        }
        StatePacket packet = new StatePacket(acceptorBallot, slotNumber, dbState);
        return  packet;
      }
    } finally {
      acceptorLock.unlock();
    }

  }

  public String getPaxosID() {
    return paxosID;
  }

  /**
   * Received request from client, so propose it to the current coordinator.
   *
   * @param json
   * @throws JSONException
   */
  public void handleRequest(JSONObject json) throws JSONException{
//        GNS.getLogger().fine(" Send request to coordinator .... ");
    // send proposal to coordinator.
//        maxSlot = -1;
//        ProposalPacket propPacket = new ProposalPacket(-1,
//                req, PaxosPacketType.PROPOSAL);
//        Ballot b = getBallot();
    int node = -1;
    try{
      acceptorLock.lock();
      if (acceptorBallot!=null) {
        node = acceptorBallot.getCoordinatorID();
      }
    } finally {
      acceptorLock.unlock();
    }
    json.put(ProposalPacket.SLOT, 0);
    json.put(PaxosPacketType.ptype, PaxosPacketType.PROPOSAL);
    json.put(PaxosManager.PAXOS_ID, paxosID);
//        if (PaxosManager.debug) {
//            for (int i = 0; i < 100; i++) {
//                PaxosManager.sendMessage(node,json);
//            }
//        }
//        else
    PaxosManager.sendMessage(node,json);
//    GNS.getLogger().fine(paxosID + "\t" +this.nodeID +
//            " Send proposal packet. Coordinator =  " + node + " Packet = " + json);
  }

  /**
   * time when the replica (as a coordinator) last made a decision.
   * @return
   */
  private long getLastDecisionTime() {
    synchronized (lastDecisionTimeLock) {
      if (lastDecisionTime == -1) lastDecisionTime = System.currentTimeMillis();
      return lastDecisionTime;
    }
  }

  private void setLastDecisionTime() {
    if (r.nextDouble() > 0.1) return;
    synchronized (lastDecisionTimeLock) {
      lastDecisionTime = System.currentTimeMillis();
    }
  }

//    /**
//     * Handle decision response from coordinator.
//     * @param prop
//     * @throws JSONException
//     */
//    private void handleDecision2(ProposalPacket prop) throws JSONException{
//
//        if (prop != null) {
//
//
//            if (prop.req != null) {
//                decisions.put(prop.slot, prop.req);
//            }
//            else {
//                PValuePacket pValuePacket = pValuesAccepted.get(prop.slot);
//                if (pValuePacket != null ) { // && pValuePacket.ballot.compareTo()
//                    decisions.put(prop.slot, pValuePacket.proposal.req);
//                }
//            }
//
//            synchronized (slotNumber) {
//                if (decisions.containsKey(slotNumber) && prop.slot == slotNumber) {
//
//                    // execute
//                    perform(decisions.get(slotNumber), slotNumber);
//                    slotNumber++;
//                    // do a combined log
//                    PaxosLogger.logDecisionAndSlotNumber(paxosID, slotNumber,prop);
//
//                    while(true) {
//                        if (decisions.containsKey(slotNumber)) {
//                            perform(decisions.get(slotNumber), slotNumber);
//                            slotNumber++;
//                            PaxosLogger.logCurrentSlotNumber(paxosID, slotNumber);
//                        }
//                        else {
//                            return;
//                        }
//                    }
//                }
//            }
//
//            if (decisions.containsKey(prop.slot))  {
//                PaxosLogger.logDecision(paxosID, new ProposalPacket(slotNumber,decisions.get(slotNumber),PaxosPacketType.DECISION));
//            }
//        }
//
////        GNS.getLogger().fine(" SLOT NUMBER BEFORE DECISION SET: " + slotNumber);
//        synchronized (slotNumberLock) {
//            while(true) {
//                if (decisions.containsKey(slotNumber)) {
//                    perform(decisions.get(slotNumber), slotNumber);
//                    slotNumber++;
//                    PaxosLogger.logCurrentSlotNumber(paxosID, slotNumber);
////                    GNS.getLogger().info("XXX " + slotNumber);
//                }
//                else {
//                    break;
//                }
//            }
//        }
//
//    }

  /**
   * Handle decision response from coordinator.
   * @param prop
   * @throws JSONException
   */
  private void handleDecision(ProposalPacket prop, JSONObject json) throws JSONException{

    if (prop != null) {
      runGC(prop.gcSlot);
//      if (prop.slot%10 == 1)
//            PaxosLogger.logDecision(paxosID,prop);

      GNS.getLogger().fine(paxosID + "\t" +nodeID +
              " Decision recvd at slot = " + prop.slot + " req = " + prop.req);
      // Add decision to slot. Union operation in Figure 1.
//      PaxosManager.addToPaxosLog(json, paxosID);
      if (decisions.size() > 20 || pValuesAccepted.size() > 20) {

        GNS.getLogger().finer(paxosID + "\t" +nodeID + " Decision size = " + decisions.size() +
                " Pvalue size = " + pValuesAccepted.size());
      }
      decisions.put(prop.slot, prop.req);
//      pValuesAccepted.remove(prop.slot);
//            if (prop.req != null) {
//
//            }
//            else {
////                PValuePacket pValuePacket = pValuesAccepted.get(prop.slot);
////                if (pValuePacket != null ) { // && pValuePacket.ballot.compareTo()
////                    decisions.put(prop.slot, pValuePacket.proposal.req);
////
////                }
//            }

    }

//        GNS.getLogger().fine(" SLOT NUMBER BEFORE DECISION SET: " + slotNumber);
    while(true) {
      synchronized (slotNumberLock) {
        if (prop!= null && prop.slot == slotNumber) {
          perform(prop.req, slotNumber);
//          decisions.remove(slotNumber);
          slotNumber++;

        }
        else if (decisions.containsKey(slotNumber)) {
          perform(decisions.get(slotNumber), slotNumber);
//          decisions.remove(slotNumber);
          slotNumber++;
        }
        else break;
      }

    }

  }

  /**
   * Apply the decision locally, e.g. write to disk.
   * @param req
   * @throws JSONException
   */
  private void perform(RequestPacket req, int slotNumber) throws JSONException{
    GNS.getLogger().info("\tPAXOS-PERFORM\t" + paxosID + "\t" + nodeID + "\t" + slotNumber  + "\t" + req.value);
    if (req.value.equals(NO_OP) ) {

      GNS.getLogger().fine(paxosID + "\t" +nodeID + " " + NO_OP + " decided in slot = " + slotNumber);
      return;
    }
//        boolean stop = PaxosManager.isStopCommand(req.value);
    if (req.isStopRequest()) {
      synchronized (stopLock) {
        isStopped = true;
      }
      GNS.getLogger().fine(" Logging paxos stop " + req);
      PaxosLogger.logPaxosStop(paxosID);
    }


    PaxosManager.handleDecision(paxosID, req);

//        GNS.getLogger().info("\tPAXOS-COMMIT\t" + paxosID + "\t" + nodeID + "\t" + slotNumber  + "\t" + req.value);

  }



  /**
   * Delete decisions which have been performed at all nodes
   * @param slotNumber slot number up to which all nodes have applied decisions.
   */
  private void deleteDecisionsAcceptedByAll(int slotNumber) {
//        System.out.println("Size of decisions = " + decisions.size());
//        GNS.getLogger().fine("Size of decisions = " + decisions.size());

    int minSlot = Integer.MAX_VALUE;
    for (int slot: decisions.keySet()) {
      if (slot < minSlot) minSlot = slot;
    }
    for (int i = minSlot; i < slotNumber; i++) // < sign is very important
      decisions.remove(i);

//        if (slotNumber == -1) {
//            GNS.getLogger().fine(paxosID + "\t" +" " +
//                    " Delete Decisions Return as slotNumber == -1");
//            return;
//        }
    // list of slots for which all nodes have accepted decisions
//        ArrayList<Integer> deleteDecisionsAtSlots = new ArrayList<Integer>();
////        int mySlotNumber;
////        synchronized (slotNumberLock) {
////            mySlotNumber = slotNumber;
////        }
//
//        for (int slot: decisions.keySet()) {
//            // is this slot before the slotnumber
//            if (slot  <  slotNumber) {
//                deleteDecisionsAtSlots.add(slot);
//            }
//        }
//        // delete decisions at slots which are accepted by all nodes.
//        for (int slot: deleteDecisionsAtSlots) {
//            RequestPacket request  = decisions.remove(slot);
////            GNS.getLogger().fine(paxosID + "\t" +"Paxos: deleting decision at slot = "
////                    + slot + " Current slot = " + mySlotNumber +" DECISION: " + request);
//        }
  }


  /**
   * Is this proposal already decided.
   * @param proposal
   * @return
   */
  private boolean isDecisionComplete(ProposalPacket proposal) {
    if (decisions.containsKey(proposal.slot) && decisions.get(proposal.slot).equals(proposal.req))
      return true;
    return false;
  }


  /**
   * Initialize the coordinator at this replica.
   */
  private void startCoordinator(long initScoutDelayMillis) {

    coordinatorBallot = new Ballot(0, getDefaultCoordinatorReplica());
    StartScoutTask task = new StartScoutTask(this);
    // coordinator starts scout after some delay to ensure that GNS has notified all replicas.

    if (getDefaultCoordinatorReplica() == nodeID) {
    // we let everyone become the coordinator
      if (initScoutDelayMillis  > 0) {
        PaxosManager.executorService.schedule(task, initScoutDelayMillis, TimeUnit.MILLISECONDS);
      } else {
        initScout();
      }
    }
  }

  private void startPreelectedCoordinator() {
    coordinatorBallot  = new Ballot(0, getDefaultCoordinatorReplica());
    if (coordinatorBallot.coordinatorID == nodeID) activeCoordinator = true;
  }
  /**
   *
   * @return
   */
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
   * @param p
   * @throws JSONException
   */
  private void handleProposal(ProposalPacket p) throws JSONException {

    // PAXOS-STOP
//        synchronized (proposalNumberLock) {
//
//        }
//        try
//        {
    if (StartNameServer.debugMode)
            GNS.getLogger().fine(paxosID + "C\t" +nodeID +
                    " Coordinator handling proposal: " + p.toJSONObject().toString());
//        } catch (JSONException e) {
//            e.printStackTrace();
//        }

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
//        else{
//            GNS.getLogger().fine(paxosID + "C\t" +nodeID +"C " +
//                    "Coordinator not active, so proposal packet DROPPED: " + p.toString());
//        }

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
//    if (StartNameServer.debugMode)

  GNS.getLogger().info(paxosID + "C\t" +nodeID + "C Ballot adopted. Handling proposals. Paxos ID = " + paxosID + " Nodes = " + nodeIDs);


    // this is executed after locking "scoutLock"

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
//                    proposals.put(slot, pValuesScout.get(slot).proposal);
          selectPValues.add(new PValuePacket(ballotScout, pValuesScout.get(slot).proposal));
          // propose stop command
          stopCommandSlot = slot;
          break;
        }
        else {
          // remove pvalue at slot, so that no-op is proposed
          pValuesScout.remove(slot);
        }
      }
      else { // if (!r.isDecisionComplete(proposals.get(slot)))
        GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C " +
                "PValue proposal for slot: " + slot);
//                proposals.put(slot, pValuesScout.get(slot).proposal);
        selectPValues.add(new PValuePacket(ballotScout, pValuesScout.get(slot).proposal));
      }
    }
    GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C " +
            "Total PValue proposals : " + selectPValues.size());

    // propose no-ops in empty slots
    int maxSlotNumberAtReplica = getMaxSlotNumberAcrossNodes();
    GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C " +
            "Max slot number at replicas = "  + maxSlotNumberAtReplica);

    int maxPValueSlot = -1;
    for (int x: pValuesScout.keySet()) {
      if (x > maxPValueSlot) {
        maxPValueSlot = x;
      }
    }
    GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C " +
            "Received pValues for slots  = " + pValuesScout.keySet() + " max slot = " + maxPValueSlot);

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
//        proposals.put(slot, proposalPacket);
    initCommander(new PValuePacket(b, proposalPacket));
    GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C Proposed NO-OP in slot = " + slot);
  }

  /**
   * Send message to acceptors with the proposed value and slot number.
   * @param pValue
   * @throws JSONException
   */
  private void initCommander(PValuePacket pValue) throws JSONException {
    // keep record of value
    ProposalStateAtCoordinator propState = new ProposalStateAtCoordinator(paxosID, pValue, nodeIDs.size());

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
        GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C" +
                " stop command proposed. in slot = " + pValue.proposal.slot);
      }
    }
    AcceptPacket accept = new AcceptPacket(this.nodeID, pValue,
            PaxosPacketType.ACCEPT, minSlot);

    GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C" +
            " Selected nodes: " + nodeIDs);

//    try {
    JSONObject jsonObject = accept.toJSONObject();
//    GNS.getLogger().fine("This is json = " + jsonObject.toString());
//    jsonObject.put(PaxosManager.PAXOS_ID, paxosID);
    PaxosManager.sendMessage(nodeIDs,jsonObject,paxosID);

    // FULLRESPONSE
//    if (StartNameServer.experimentMode) GNS.getLogger().severe("\t" + paxosID + "\t" + pValue.proposal.slot + "\t" + pValue.proposal.req.isStopRequest() + "\tRequestProposed" );


//    if (StartNameServer.experimentMode)
//      GNS.getLogger().severe("\t" + paxosID + "\t" + pValue.proposal.slot + "\t" + pValue.proposal.req.isStopRequest() + "\tRequestProposed" );
//>>>>>>> .r208

//    handleIncomingMessage(jsonObject, PaxosPacketType.ACCEPT);
//      PaxosManager.tcpTransport.sendToIDs(nodeIDs,jsonObject);
//      sendMessage();
//      if (PaxosManager.debug) {
//        GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C" +
//                " Sent packet to acceptors: " + jsonObject);
//
//      }
//      else {
//        for (int x: nodeIDs)
//          PaxosManager.sendMessage(x,jsonObject);
//      }
//
//    } catch (IOException e) {
//      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//    }

//        for (Integer i: nodeIDs) {
//            // create accept packet
//
//            // send to acceptors/replicas
//            sendMessage(i, accept);
////            GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C " +
////                    "sending accept msg to "+ i +
////                    " msg: " + accept.toJSONObject().toString());
//            //if this command was stop command update the variable
//            // PAXOS-STOP: if this command is the stop command,
//
//        }
//        }
  }

  /**
   *
   * @param replyPacket
   */
  private void handleSyncReplyPacket(SynchronizeReplyPacket replyPacket) {
//        GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C here...");
    updateNodeAndSlotNumbers(replyPacket);

    if (replyPacket.missingSlotNumbers != null) {
//      ArrayList<ProposalPacket> missingPackets = new ArrayList<ProposalPacket>();
//      boolean allMissingPacketsInMemory = false;
      for (int x: replyPacket.missingSlotNumbers) {
        if (decisions.containsKey(x)) {
//                    GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C " +
//                            "Sending decision for slot = " + x + " to node " + replyPacket.nodeID);
          RequestPacket decision = decisions.get(x);
          if (decision == null) continue;
          ProposalPacket packet = new ProposalPacket(x, decision, PaxosPacketType.DECISION, 0);
//          missingPackets.add(packet);
          sendMessage(replyPacket.nodeID, packet);
        }
        else {
          // TODO read from db and send state to node.
//          GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C " +
//                  "Missing decision not found at coordinator for slot = " + x + " Decision: " + decisions.keySet());
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
   * @param accept
   * @throws JSONException
   */
  private void handleAcceptMessageReply(AcceptReplyPacket accept) throws JSONException{
    // resend missing requests
//        updateNodeAndSlotNumbers(accept.nodeID, accept.slotNumberAtReplica);

    int slot = accept.slotNumber;
    Ballot b = accept.ballot;
    int senderNode = accept.nodeID;
//        int deletelsot = accept.commitSlot;
//        updateNodeAndSlotNumbers(senderNode, deletelsot);
    GNS.getLogger().fine(paxosID + "C\t" + nodeID + "C Accept-Reply\tsender\t" + accept.nodeID
            + " slot\t" + slot);//+  "  accept = " + accept.toJSONObject().toString());
    ProposalStateAtCoordinator stateAtCoordinator = pValuesCommander.get(slot);

    if (stateAtCoordinator == null) {
//            GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C " +
//                    "Commander not found for slot =  " + slot + ". Either decision complete or ballot preempted.");
      return;
    }
//        if (stateAtCoordinator.pValuePacket.proposal.req.equals(accept.pValue.proposal.req)) {
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
        //                    waitForCommander.remove(slot);
        GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C " +
                "higher ballot recvd. current ballot preempted.");
        try {
          acceptorLock.lock();
          synchronized (coordinatorBallotLock) {
            if (coordinatorBallot.compareTo(stateAtCoordinator.pValuePacket.ballot) == 0 && activeCoordinator == true) {
              activeCoordinator = false;
              coordinatorBallot = b;
              pValuesCommander.clear();
            }
          }
          if (stateAtCoordinator.pValuePacket.ballot.compareTo(acceptorBallot) > 0) {
            acceptorBallot = new Ballot(b.ballotNumber, b.coordinatorID);
//                        PaxosLogger.logCurrentBallot(paxosID, acceptorBallot);
          }
        }   finally {
          acceptorLock.unlock();
        }
      }
//                    handlePreempted(accept.pValues.ballot);
    }
//            else {
//                GNS.getLogger().severe(paxosID + "C\t" +nodeID + "C " +
//                        "Lower ballot recvd. ERROR THIS CASE SHOULD NOT HAPPEN! Recevd ballot = " + accept.pValue.ballot
//                        + " Proposed ballot = " + stateAtCoordinator.pValuePacket.ballot);
    // this case should not arise
//            }
//        }
  }

  /**
   * handle response of accept message from an acceptor.
   * @param accept
   * @throws JSONException
   */
  private void handleAcceptMessageReply2(AcceptPacket accept) throws JSONException{
    // resend missing requests
//        updateNodeAndSlotNumbers(accept.nodeID, accept.slotNumberAtReplica);

    int slot = accept.pValue.proposal.slot;
//        GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C recvd Accept-Reply msg from " + accept.nodeID
//                + " slot = " + slot +  "  accept = " + accept.toJSONObject().toString());
    ProposalStateAtCoordinator stateAtCoordinator = pValuesCommander.get(slot);

    if (stateAtCoordinator == null) {
//            GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C " +
//                    "Commander not found for slot =  " + slot + ". Either decision complete or ballot preempted.");
      return;
    }
//        if (stateAtCoordinator.pValuePacket.proposal.req.equals(accept.pValue.proposal.req)) {
    if (accept.pValue.ballot.compareTo(stateAtCoordinator.pValuePacket.ballot) == 0) {
      // case: replica accepted proposed valye
//                GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C ballot OK. ");
      stateAtCoordinator.addNode(accept.nodeID);
//                GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C commander accept count: " +
//                        stateAtCoordinator.getNodesResponded() + " nodes "+ nodeIDs.size());

      sendDecisionForSlot(slot, stateAtCoordinator);


    }
    else if (accept.pValue.ballot.compareTo(stateAtCoordinator.pValuePacket.ballot) > 0){
      // case: ballot preempted
      stateAtCoordinator = pValuesCommander.remove(slot);
      if (stateAtCoordinator != null) {
        PaxosManager.removeFromActiveProposals(stateAtCoordinator);
        //                    waitForCommander.remove(slot);
        GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C " +
                "higher ballot recvd. current ballot preempted.");
        try {
          acceptorLock.lock();
          synchronized (coordinatorBallotLock) {
            if (coordinatorBallot.compareTo(stateAtCoordinator.pValuePacket.ballot) == 0 && activeCoordinator == true) {
              activeCoordinator = false;
              coordinatorBallot = accept.pValue.ballot;
              pValuesCommander.clear();
            }
          }
          if (stateAtCoordinator.pValuePacket.ballot.compareTo(acceptorBallot) > 0) {
            acceptorBallot = new Ballot(accept.pValue.ballot.ballotNumber, accept.pValue.ballot.coordinatorID);
//                        PaxosLogger.logCurrentBallot(paxosID, acceptorBallot);
          }
        }   finally {
          acceptorLock.unlock();
        }
      }
//                    handlePreempted(accept.pValues.ballot);
    }
//            else {
//                GNS.getLogger().severe(paxosID + "C\t" +nodeID + "C " +
//                        "Lower ballot recvd. ERROR THIS CASE SHOULD NOT HAPPEN! Recevd ballot = " + accept.pValue.ballot
//                        + " Proposed ballot = " + stateAtCoordinator.pValuePacket.ballot);
    // this case should not arise
//            }
//        }
  }


  void logFullResponseAfterStop() {
    // FULLRESPONSE
//    if (pValuesCommander.size() > 0) {
//      ConcurrentHashMap c = pValuesCommander;
//      pValuesCommander = new ConcurrentHashMap<Integer, ProposalStateAtCoordinator>();
//      for (Object o: c.keySet()) {
//        ProposalStateAtCoordinator v = (ProposalStateAtCoordinator) c.get(o);
//        if (StartNameServer.experimentMode) GNS.getLogger().severe("\t" + paxosID + "\t" + v.pValuePacket.proposal.slot
//                + "\t" + v.pValuePacket.proposal.req.isStopRequest()+  "\tFullResponseAfterStop");
//      }
//    }
  }

  private void sendDecisionForSlot(int slot, ProposalStateAtCoordinator stateAtCoordinator) {
    // check if majority reached
    // FULLRESPONSE
//    if (stateAtCoordinator.isFullResponse()) {
//      PaxosManager.removeFromActiveProposals(stateAtCoordinator);
//      Object o = pValuesCommander.remove(slot);
//      if (o != null && StartNameServer.experimentMode) {
//        GNS.getLogger().info("\t" + paxosID + "\t" + slot + "\t" + stateAtCoordinator.pValuePacket.proposal.req.isStopRequest() + "\tFullResponse");
//      }
//      return;
//    }

    if (stateAtCoordinator.isMajorityReached() == false) {
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
  public void initScout() {

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
//            updateBallot(b);
      }finally {
        acceptorLock.unlock();
      }
//<<<<<<< .mine
      GNS.getLogger().severe(paxosID + "C\t" + nodeID + "C Coordinator proposing new ballot: " + ballotScout);
//=======
//            GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C Coordinator proposing new ballot: " + ballotScout);
//>>>>>>> .r208
//            scoutingInProgress = true;
      waitForScout = new ArrayList<Integer>();
      pValuesScout = new HashMap<Integer,PValuePacket>();
    }finally {
      scoutLock.unlock();
    }

    // create prepare packet
    PreparePacket prepare = new PreparePacket(nodeID, 0, ballotScout, PaxosPacketType.PREPARE);
//		for (int i = 1; i <= Network.N; i++) {
    for (Integer i: nodeIDs) {

      prepare.receiverID = i;
//            GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C sending prepare msg to " +i );
      sendMessage(i, prepare);
    }

    int numberRetry = 50; //(int) (FailureDetection.timeoutIntervalMillis / PaxosManager.RESEND_PENDING_MSG_INTERVAL_MILLIS);
    CheckPrepareMessageTask task = new CheckPrepareMessageTask(this,prepare, ballotScout, numberRetry);

    PaxosManager.executorService.scheduleAtFixedRate(task,PaxosManager.RESEND_PENDING_MSG_INTERVAL_MILLIS,
            PaxosManager.RESEND_PENDING_MSG_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);

  }

  /**
   * Returns true if there is a need to resend the prepare message again, otherwise false.
   * @param proposedBallot
   * @param prepare
   * @return
   */
  public boolean resendPrepare(Ballot proposedBallot, PreparePacket prepare) {
    try {
      scoutLock.lock();
      if (proposedBallot.equals(ballotScout) && waitForScout != null) {
        // what if majority have failed.
        int nodesUp = 0;
        for (int x: nodeIDs)
          if (FailureDetection.isNodeUp(x)) nodesUp++;
        if (nodesUp*2 < nodeIDs.size()) return false; // more than half node down, give up resending prepare
        boolean resend = false;
        for (int x: nodeIDs) {
          if (waitForScout.contains(x) == false && FailureDetection.isNodeUp(x)) {
            sendMessage(x, prepare); // send message to the node if it is up and has not responded.
            resend = true;
            GNS.getLogger().fine(paxosID + "\t" + nodeID + "C\t Ballot = " + proposedBallot + " resend to node " + x);
          }
        }
        return resend;
      } else {
        GNS.getLogger().severe(paxosID + "\t" + nodeID + "C\t No need to resend PreparePacket. Ballot adopted = " + proposedBallot);
        return false;
      }
    } finally {
      scoutLock.unlock();
    }

  }

  /**
   * Updates slotNumber for nodeID in data structure nodeAndSlotNumbers
   * @param nodeID
   * @param slotNumber
   */
  private void updateNodeAndSlotNumbers(int nodeID, int slotNumber) {
//        synchronized (nodeAndSlotNumbers) {
    if (!nodeAndSlotNumbers.containsKey(nodeID) ||
            slotNumber > nodeAndSlotNumbers.get(nodeID)) {
//            GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C Slot number for " + nodeID
//                    + " updated to node " + slotNumber);
      nodeAndSlotNumbers.put(nodeID, slotNumber);
    }
//        }
  }

  private static final ReentrantLock minSlotLock = new ReentrantLock();

  int minSlotNumberAcrossNodes = 0;

  /**
   * Updates nodeAndSlotNumbers based on replyPacket
   * @param
   */
  private void updateNodeAndSlotNumbers(SynchronizeReplyPacket replyPacket) {
//        if (replyPacket.maxDecisionSlot ==  -1) {
//            return;
//        }

    int minSlot = Integer.MAX_VALUE;
    if (replyPacket.missingSlotNumbers != null) {
      for (int x: replyPacket.missingSlotNumbers) {
        if (minSlot > x) minSlot = x;
      }
    }
    else {
      minSlot = replyPacket.maxDecisionSlot;
    }

    if (minSlot == Integer.MAX_VALUE) return;

    minSlot += 1;

    int node = replyPacket.nodeID;

//        synchronized (nodeAndSlotNumbers) {
    if (!nodeAndSlotNumbers.containsKey(node) ||
            minSlot > nodeAndSlotNumbers.get(node)) {
//            GNS.getLogger().fine(paxosID + "C\t" +node + "C node = " + node
//                    + " Update: slot number = " + minSlot);
      nodeAndSlotNumbers.put(node, minSlot);
    }
    synchronized (minSlotLock){
      minSlotNumberAcrossNodes = getMinSlotNumberAcrossNodes();
    }

//        }
  }

  /**
   * handle reply from a replica for a prepare message.
   * @param prepare
   * @throws JSONException
   */
  private void handlePrepareMessageReply(PreparePacket prepare) throws JSONException{

    updateNodeAndSlotNumbers(prepare.receiverID, prepare.slotNumber);

    try{scoutLock.lock();
//        synchronized (scoutingInProgress) {
      if (waitForScout == null) {
//                GNS.getLogger().fine(paxosID + "C\t" + nodeID + "C not getting ballot accepted now.");
        return;
      }
//            GNS.getLogger().fine(paxosID + "C\t" +nodeID
//                    + "C received Prepare-Reply Msg "+prepare.toJSONObject().toString());
      if (waitForScout.contains(prepare.receiverID)) {
//                GNS.getLogger().fine(paxosID + "C\t" +nodeID
//                        + "C already received prepare response from " + prepare.receiverID);
        return;
      }

      if (prepare.ballot.compareTo(ballotScout) < 0) {
//                GNS.getLogger().fine(paxosID + "C\t" +nodeID
//                        + "C received response for a earlier scout. Ballot scout = " + ballotScout
//                        + " Message ballot = " + prepare.ballot);
        return;
      }
      else if (prepare.ballot.compareTo(ballotScout) > 0) {
//                GNS.getLogger().fine(paxosID + "C\t" +nodeID  + "C Ballot pre-empted. ");
        waitForScout = null;
//                scoutingInProgress = false;
//                handlePreempted(prepare.ballot);
      }
      else {
//                GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C Ballot accepted " +
//                        ballotScout + " by node " + prepare.receiverID);
        for(PValuePacket pval : prepare.accepted.values()) {
          int slot = pval.proposal.slot;
          if (!pValuesScout.containsKey(slot) ||
                  prepare.ballot.compareTo(pValuesScout.get(slot).ballot) > 0)  {
            pValuesScout.put(slot, pval);
//                        GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C " +
//                                "pValue from node " + prepare.receiverID + "added to list: " + pval.toJSONObject());
          }
        }

        waitForScout.add(prepare.receiverID);
//                GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C " +
//                        "Scout prepare msg count: " + waitForScout.size());
        if (waitForScout.size() > nodeIDs.size() / 2) { // activeCoordinator == false &&
          // case: ballot accepted by majority, cooridinator elected
//                    GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C " +
//                            "Scout received majority votes for ballot. " + ballotScout + "Votes = "
//                            + waitForScout.size() + " Nodes =  " + nodeIDs.size());
          waitForScout = null;
//                    scoutingInProgress = false;
          handleAdoptedNew();
        }
      }
    }finally {
      scoutLock.unlock();
    }
  }

  private Ballot resendPrepareMessage(int nodeID) {
    try{scoutLock.lock();
//        synchronized (scoutingInProgress) {
      if (waitForScout != null) {
        return ballotScout;
      }
//        }
    }finally {
      scoutLock.unlock();
    }
    return null;
  }

  /**
   * message reporting node failure.
   * @param packet
   */
  private void handleNodeStatusUpdate(FailureDetectionPacket packet) {
//    updateSelectNodes();
    if (packet.status == true) return; // node is up.
    try {
      assert packet.responderNodeID != nodeID;
    } catch (Exception e) {
      GNS.getLogger().severe("Exception Failure detection reports that this node has failed. Not possible. " + packet);
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
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
            && getDefaultCoordinatorReplica() == nodeID) { // I am next coordinator

      GNS.getLogger().warning(paxosID + "C\t" + nodeID + " Coordinator failed\t" + packet.responderNodeID + " Propose new ballot.");

      if (StartNameServer.debugMode)
        GNS.getLogger().fine(paxosID + "C\t" +nodeID +"C coordinator has failed " + coordinatorID);
      if (StartNameServer.debugMode)
        GNS.getLogger().fine(paxosID + "C\t" +nodeID + " current ballot coordinator failed " + coordinatorID);
      // I should try to get a new ballot accepted.
      initScout();
    }
  }

  /**
   * Minimum slot number in the list: nodeAndSlotNumbers.
   * @return
   */
  private int getMinSlotNumberAcrossNodes( ) {
//    if (nodeAndSlotNumbers.size() < nodeIDs.size()) return -1;
    int minSlot = -1;
    for (int nodeID: nodeAndSlotNumbers.keySet()) {
      if (FailureDetection.isNodeUp(nodeID) == false) continue;
      if (minSlot == -1 || minSlot > nodeAndSlotNumbers.get(nodeID))  {
        minSlot = nodeAndSlotNumbers.get(nodeID);
      }
    }
    return minSlot;
  }

  /**
   * New ballot proposed by a replica, handle the proposed ballot.
   * @param packet
   * @throws JSONException
   */
  private void handlePrepare(JSONObject incomingJson, PreparePacket packet) throws JSONException {
//        PaxosLogger.logCurrentBallot(paxosID, packet.ballot);
    //
    GNS.getLogger().fine(paxosID + "\t" +nodeID + " Acceptor received PreparePacket: " + packet.toJSONObject().toString());
//        Ballot b1 = null;
    try{
      acceptorLock.lock();
//        synchronized (acceptorBallot) {
//                b1 = new Ballot(acceptorBallot.getBallotNumber(), acceptorBallot.getCoordinatorID());
      if (acceptorBallot == null || packet.ballot.compareTo(acceptorBallot) > 0) {
        acceptorBallot = packet.ballot;

      }

//            sendMessage(packet.coordinatorID,
//                    packet.getPrepareReplyPacket(acceptorBallot, pValuesAccepted, slot));
      // Log this message and send reply to coordinator

      Packet p = packet.getPrepareReplyPacket(acceptorBallot, nodeID, pValuesAccepted, slotNumber);
//
//      PaxosManager.addToPaxosLog(incomingJson,paxosID);
//      sendMessage(packet.coordinatorID,p);

      PaxosLogger.logMessage(new LoggingCommand(paxosID, incomingJson, LoggingCommand.LOG_AND_SEND_MSG, packet.coordinatorID, p.toJSONObject()));

//            b1 = new Ballot(acceptorBallot.getBallotNumber(), acceptorBallot.getCoordinatorID());
//        }
    }finally {
      acceptorLock.unlock();
    }

    GNS.getLogger().fine(paxosID + "\t" +"Reached here.");

  }

  Random r = new Random();

  /**
   * TODO: document this optimization.
   */
  AcceptReplyPacket acceptReply;

  /**
   * Handle accept messages from coordinator
   * @param accept
   */
  private void handleAccept(JSONObject incomingJson, AcceptPacket accept) {
    try {
      GNS.getLogger().fine(paxosID + "\t" + nodeID + "\tAcceptorslot\t" + accept.pValue.proposal.slot);

//            PaxosLogger.logPValue(paxosID, accept.pValue);

      Ballot b1 = null;
      try {
        acceptorLock.lock();
        if (accept.pValue.ballot.compareTo(acceptorBallot) == 0) { // accept the pvalue

          pValuesAccepted.put(accept.pValue.proposal.slot, accept.pValue);
//                    b1 = acceptorBallot;
          if (acceptReply == null || acceptReply.ballot.equals(accept.pValue.ballot) == false)
            acceptReply = new AcceptReplyPacket(nodeID, acceptorBallot, accept.pValue.proposal.slot);
          else {
            acceptReply.slotNumber = accept.pValue.proposal.slot;
          }
//                    AcceptReplyPacket acceptReply = new AcceptReplyPacket(nodeID, b1,accept.pValue.proposal.slot);
//            AcceptPacket p = accept.getAcceptReplyPacket(nodeID, b1);
//                    sendMessage(accept.nodeID,acceptReply);

        } else if (accept.pValue.ballot.compareTo(acceptorBallot) > 0) {  // accept the pvalue and the ballot

          acceptorBallot = new Ballot(accept.pValue.ballot.ballotNumber, accept.pValue.ballot.coordinatorID);
          b1 = acceptorBallot;
//                    PaxosLogger.logCurrentBallot(paxosID, acceptorBallot);
          pValuesAccepted.put(accept.pValue.proposal.slot, accept.pValue);
          acceptReply = new AcceptReplyPacket(nodeID, b1,accept.pValue.proposal.slot);
//            AcceptPacket p = accept.getAcceptReplyPacket(nodeID, b1);
//                    sendMessage(accept.nodeID,acceptReply);
        } else { // accept.pValue.ballot.compareTo(acceptorBallot) < 0)
          // do not accept, reply with own acceptor ballot
          b1 = new Ballot(acceptorBallot.ballotNumber, acceptorBallot.coordinatorID);
          acceptReply = new AcceptReplyPacket(nodeID, b1,accept.pValue.proposal.slot);
//            AcceptPacket p = accept.getAcceptReplyPacket(nodeID, b1);
//                    sendMessage(accept.nodeID,acceptReply);
        }
        // new: log and send message.

//        PaxosManager.addToPaxosLog(incomingJson,paxosID);
//        sendMessage(accept.nodeID, acceptReply);

        PaxosLogger.logMessage(new LoggingCommand(paxosID, incomingJson, LoggingCommand.LOG_AND_SEND_MSG, accept.nodeID, acceptReply.toJSONObject()));

//                if (acceptorBallot == null || accept.pValue.ballot.compareTo(acceptorBallot) >= 0) {
//                    if (accept.pValue.ballot.compareTo(acceptorBallot) > 0) {
//                         GNS.getLogger().fine(paxosID + "\t" +nodeID +
//                                " Acceptor: updated ballot number to: " + accept.pValue.ballot);
//                        acceptorBallot = new Ballot(accept.pValue.ballot.ballotNumber, accept.pValue.ballot.coordinatorID);
//                        PaxosLogger.logCurrentBallot(paxosID, acceptorBallot);
//                    }
//                    pValuesAccepted.put(accept.pValue.proposal.slot, accept.pValue);
//                    PaxosLogger.logPValue(paxosID, accept.pValue);
//                }
//                synchronized (garbageCollectionLock) {
//                  if (slotAvailableForGC < accept.slotNumberAtReplica) slotAvailableForGC = accept.slotNumberAtReplica;
//                }
      } finally {
        acceptorLock.unlock();
      }
      // slot ballot node

//      if (accept.pValue.proposal.slot%20 == 0) runGC(accept.slotNumberAtReplica);



//             GNS.getLogger().fine(paxosID + "\t" +nodeID +
//                    " ACCEPTORAcceptor: after locks : slot = " + accept.pValue.proposal.slot);

//            int slot;
//            synchronized (slotNumberLock) {
//                slot = slotNumber;
//            }
//            accept.makeAcceptReplyPacket(nodeID,slot,b1);


//             GNS.getLogger().fine(paxosID + "\t" +nodeID +
//                    " ACCEPTORAcceptor: SEND REPLY : to " + accept.nodeID +
//                    " for slot = " + accept.pValue.proposal.slot + " Packet = " + accept);
      // GC code here





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
        garbageCollectionSlot = suggestedGCSlot;
      }
      else return;
    }
//            System.out.println("Checking gc "  + gc);

    int coordinatorID;
    synchronized (acceptorBallot) {
      coordinatorID = acceptorBallot.getCoordinatorID();
    }
//                System.out.println("DOING gc");
//                PaxosLogger.logGarbageCollectionSlot(paxosID, accept.slotNumberAtReplica);
    deletePValuesDecided(suggestedGCSlot);
    deleteDecisionsAcceptedByAll(suggestedGCSlot);
    SynchronizeReplyPacket synchronizeReplyPacket = getSyncReply(false);
    sendMessage(coordinatorID, synchronizeReplyPacket);
//                System.out.println("Sync reply packet = " + synchronizeReplyPacket);


  }

// REMVOE THIS CODE
//    public void doGarbageCollection() {
//        boolean gc = false;
//        int gcSlot = 0;
//        synchronized (garbageCollectionLock) {
//            if (garbageCollectionSlot < slotAvailableForGC) {
//                gc = true;
//                gcSlot = garbageCollectionSlot = slotAvailableForGC;
//            }
//        }
//
//        if (gc) {
//            int node;
//            synchronized (acceptorBallot) {
//                if (acceptorBallot == null) return;
//                node = acceptorBallot.getCoordinatorID();
//            }
////                PaxosLogger.logGarbageCollectionSlot(paxosID, accept.slotNumberAtReplica);
//            SynchronizeReplyPacket synchronizeReplyPacket = getSyncReply(false);
//            sendMessage(node, synchronizeReplyPacket);
//            deletePValuesDecided(gcSlot);
//            deleteDecisionsAcceptedByAll(gcSlot);
//        }
//    }

  /**
   * handle sync request message from a coordinator,
   * return the current missing slot numbers and maximum slot number for which decision is received
   * @param packet
   */
  private void handleSyncRequest(SynchronizePacket packet) {
    SynchronizeReplyPacket synchronizeReplyPacket = getSyncReply(true);
    GNS.getLogger().fine(paxosID + "\t" +nodeID + " " +
            "Handling Sync Request: " + synchronizeReplyPacket);
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

    SynchronizeReplyPacket synchronizeReplyPacket = new SynchronizeReplyPacket(nodeID,
            maxDecision, missingSlotNumbers, flag);

    return synchronizeReplyPacket;
  }




  /**
   * Delete pValues at slots less than slotNumber.
   * @param slotNumber all nodes have received all decisions for slots less than "slotNumber"
   *
   */
  private void deletePValuesDecided(int slotNumber) {
    GNS.getLogger().fine("Size of pvalues = " + pValuesAccepted.size());
//    System.out.println("Size of pvalues = " + pValuesAccepted.size());

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
//        ArrayList<Integer> deletePValuesAtSlots = new ArrayList<Integer>();
//        for (int slot: pValuesAccepted.keySet()) {
//            if (slot < slotNumber) { // less than operator is very important
//                deletePValuesAtSlots.add(slot);
//            }
//        }
//        for (int slot: deletePValuesAtSlots) {
//            PValuePacket pValue = pValuesAccepted.remove(slot);
//            GNS.getLogger().fine(paxosID + "\t" +"PAXOS: deleting PValue at slot = " + slot +
//                    " Current slot = " + slotNumber +  " PValue = " + pValue);
//
//        }

  }

  /**
   * if replica is coordinator,
   * resend accept messages for slots who which haven't received accept-reply from majority of replicas
   * {@code RESEND_TIMEOUT} interval
   */
  public void resendPendingAccepts() {

    synchronized (coordinatorBallotLock) {
      if (!activeCoordinator) return;
    }
    int minSlot;
    synchronized (minSlotLock) {
      minSlot = minSlotNumberAcrossNodes;
    }

    for (int slot: pValuesCommander.keySet()) {
      ProposalStateAtCoordinator state = pValuesCommander.get(slot);
      if (state == null) continue;
      // already reached majority
      if (state.getNodesResponded()*2 > nodeIDs.size()) continue;

      // resend ACCEPT to those who haven't yet responded
      if (state.getTimeSinceAccept() >= RESEND_TIMEOUT) {
        GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C " +
                "Accept Pending for Slot = " + slot + " Resending Messages.");
        state.updateAcceptSentTime();
        for (int node: nodeIDs) {
          // ACCEPT-REPLY received from node?
          if (!state.hasNode(node)) {
            AcceptPacket accept = new AcceptPacket(this.nodeID, state.pValuePacket,
                    PaxosPacketType.ACCEPT, minSlot);
            sendMessage(node, accept);
          }
        }
      }
    }
  }

  public boolean resendPendingProposal(ProposalStateAtCoordinator state) {
//    if (state.getNodesResponded()*2 > nodeIDs.size()) return false;
    try {
      acceptorLock.lock();
      if (StartNameServer.experimentMode)
      GNS.getLogger().info("\tResendingMessage\t" + state.paxosID + "\t" +
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

  /**
   * if replica is coordinator,
   * check if all replicas have received the decisions until the last slot number decided.
   */
  public void checkIfReplicasUptoDate() {

    synchronized (coordinatorBallotLock) {
      if (!activeCoordinator) return;
    }

    // if (last decision is made less than 1 sec ago): continue
    if (System.currentTimeMillis() - getLastDecisionTime() < RESEND_TIMEOUT) {
      return;
    }


    int minSlot;
    synchronized (minSlotLock) {
      minSlot = minSlotNumberAcrossNodes;
    }


    int slotNumberCopy;
    synchronized (slotNumberLock) {
      slotNumberCopy = slotNumber;
    }
    if (minSlot == slotNumberCopy) {
      return;
    }

    GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C " +
            "Sending sync messages Min slot = " +  minSlot + " Slot number = " + slotNumberCopy);

    // if (all replicas excluding failed ones are up to date): continue
    SynchronizePacket synchronizePacket = new SynchronizePacket(nodeID);
    for (int x: nodeIDs) {
      // ignore if node is detected as failed
      if (!FailureDetection.isNodeUp(x)) continue;
      // send a request to send current replicas if it is not up to date
      if (nodeAndSlotNumbers.get(x) < slotNumberCopy) {
//                GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C " +
//                        "Sending to node = " +  x);
        sendMessage(x, synchronizePacket);
      }
    }
  }
}



class ProposalStateAtCoordinator implements Comparable{

  public String paxosID;

  private long acceptSentTime = -1;
  PValuePacket pValuePacket;
  private ConcurrentHashMap<Integer, Integer> nodes;
  int numReplicas;
  public ProposalStateAtCoordinator(String paxosID, PValuePacket pValuePacket, int numReplicas) {
    this.paxosID = paxosID;
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
    if (majority == true)  return false;

    if (nodes.size() * 2 > numReplicas) {
      majority = true;
      return true;
    }
    return false;
  }

  private boolean fullResponse = false;

  public synchronized boolean isFullResponse() {
    if (fullResponse == true || majority == false)  return false;

    if (nodes.size() == numReplicas) {
      fullResponse = true;
      return true;
    }
    return false;
  }

  public int getNodesResponded() {
    return nodes.size();
  }

  public boolean hasNode(int node) {
    return nodes.containsKey(node);
  }


  public long getTimeSinceAccept() {
    if (acceptSentTime == -1) acceptSentTime = System.currentTimeMillis();
    return  System.currentTimeMillis() - acceptSentTime;
  }

  public void updateAcceptSentTime() {
    this.acceptSentTime = System.currentTimeMillis();
  }


  @Override
  public int compareTo(Object o) {
    ProposalStateAtCoordinator p = (ProposalStateAtCoordinator)o;
    if (acceptSentTime < p.acceptSentTime) return -1;
    else if (acceptSentTime > p.acceptSentTime) return 1;
    return 0;
  }
}


class CheckPrepareMessageTask extends TimerTask {

  int numRetry;
  int count = 0;
  PaxosReplica replica;
  PreparePacket preparePacket;
  Ballot proposedBallot;

  public CheckPrepareMessageTask(PaxosReplica replica, PreparePacket preparePacket, Ballot proposedBallot, int numRetry) {
    this.numRetry = numRetry;
    this.replica = replica;
    this.preparePacket = preparePacket;
    this.proposedBallot = proposedBallot;

  }

  @Override
  public void run() {
    try {
      if (replica.isStopped()) throw  new RuntimeException();
      boolean sendAgain = replica.resendPrepare(proposedBallot,preparePacket);
      if (sendAgain == false || count == numRetry) throw  new RuntimeException();
    } catch (Exception e) {
      GNS.getLogger().severe("Exception in Check prepare message task. " + e.getMessage());
      e.printStackTrace();
    }
  }
}


class StartScoutTask extends TimerTask {
  PaxosReplica replica;
  public StartScoutTask(PaxosReplica replica) {
    this.replica = replica;

  }

  @Override
  public void run() {
    try{
      replica.initScout();
    }catch (Exception e) {
      GNS.getLogger().severe("Exception in start scout .. " + e.getMessage());
      e.printStackTrace();
    }
  }
  
}