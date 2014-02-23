package edu.umass.cs.gns.paxos;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartNameServer;
import edu.umass.cs.gns.packet.paxospacket.*;
import edu.umass.cs.gns.util.OutputMemoryUse;
import edu.umass.cs.gns.util.Util;
import net.sourceforge.sizeof.SizeOf;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Implements a memory-efficient paxos. On a normal replica, a paxos instance consumes 250 bytes, a coordinator uses
 * 400 bytes.
 * Created by abhigyan on 1/9/14.
 */
public class PaxosReplicaNew extends PaxosReplicaInterface{

  /**
   * Special no-op command.
   */
  static final String NO_OP = "NO_OP";

  /**
   * ID of the paxos instance this replica belongs to.
   */
  String paxosID; // can be replaced with char-array

  int version; // will replace paxos ID field

  short[] nodeIDs;

  int slotNumber = 0;

  static int nodeID;

  /**
   * Stores both accept and commit messages at a replica.
   */
  ArrayList<Object> replicaMessages = null;
//  HashMap<Integer, RequestPacket> decisions = null;

//  int garbageCollectionSlot = 0;

//  long lastGarbageCollectionTime = 0;

  Ballot acceptorBallot;

  boolean isStopped = false;

  PaxosCoordinator coordinator;

  PaxosManager paxosManager;

  /**
   * Constructor.
   * @param paxosID ID of this paxos group
   * @param ID  ID of this node
   * @param nodeIDs1 set of IDs of nodes in this paxos group
   */
  public PaxosReplicaNew(String paxosID, int ID, Set<Integer> nodeIDs1, PaxosManager paxosManager) {
//    System.out.println("In constructor...");
    this.paxosID = paxosID;

    PaxosReplicaNew.nodeID = ID;

    nodeIDs = new short[nodeIDs1.size()];
    int i = 0;
    for (int x: nodeIDs1) {
      nodeIDs[i] = (short) x;
      i++;
    }

    acceptorBallot = new Ballot(0, getInitialCoordinatorReplica());

    if (acceptorBallot.coordinatorID == nodeID) {
      coordinator = new PaxosCoordinator();
      coordinator.coordinatorBallot = new Ballot(acceptorBallot.ballotNumber, acceptorBallot.coordinatorID);
    }
    replicaMessages = new ArrayList<Object>(4);
    this.paxosManager = paxosManager;
  }

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
      LoggingCommand logCmd = null;
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
          handleDecision(proposal, false);
          break;
        // coordinator --> replica
        case PaxosPacketType.PREPARE:
          PreparePacket prepare = new PreparePacket(json);
          logCmd = handlePrepare(json, prepare);
          break;
        // replica --> coordinator
        case PaxosPacketType.PREPARE_REPLY:
          prepare = new PreparePacket(json);
          handlePrepareMessageReply(prepare);
          break;
        // coordinator --> replica
        case PaxosPacketType.ACCEPT:
          AcceptPacket accept = new AcceptPacket(json);
          logCmd = handleAccept(json, accept);

          break;
        // replica --> coordinator
        case PaxosPacketType.ACCEPT_REPLY:
          AcceptReplyPacket acceptReply = new AcceptReplyPacket(json);
          logCmd = handleAcceptMessageReply(acceptReply);
          break;
        // replica --> replica
        case PaxosPacketType.RESEND_ACCEPT:
          handleResendAccept(json);
//        case PaxosPacketType.SEND_STATE:
//        case PaxosPacketType.SEND_STATE_NO_RESPONSE:
////          SendCurrentStatePacket sendState = new SendCurrentStatePacket(json);
////          handleSendState(sendState);
//          handleSendState(new SendCurrentStatePacket2(json));
//          break;

        // local failure detector --> replica
        case PaxosPacketType.NODE_STATUS:
          //				GNRS.getLogger().fine("XXXRecvd node status msg");
          FailureDetectionPacket fdPacket = new FailureDetectionPacket(json);
          handleNodeStatusUpdate(fdPacket);
          break;
        //				GNRS.getLogger().fine(ID + " received failure detection packet.
        // 				Message: " + json.toString());
        //				coordinator.handleAcceptMessage(fdPacket);
//        case PaxosPacketType.SYNC_REQUEST:
//          handleSyncRequest(new SynchronizePacket(json));
//          break;
        case PaxosPacketType.SYNC_REPLY:
          handleSyncReplyPacket(new SynchronizeReplyPacket(json));
          break;
        default:
          GNS.getLogger().severe("Unrecognized packet type: " + json);
      }
//            else if (incomingPacketType == PaxosPacketType.REQUEST_STATE) {
//                synchronize with other replicas
//                sendCurrentStateToAllNodes();
//                requestStateTaskScheduled = false;
//            }
      if (logCmd != null) {
        paxosManager.paxosLogger.logMessage(logCmd);
      }
    } catch (JSONException e) {
      e.printStackTrace();
    }
//        if (takeOverAsCoordinator()) {
//            GNS.getLogger().fine(paxosID + "\t" + " Taking over as coordinator. msg count = " + System.currentTimeMillis());
//            initScout();
//        }
  }

  private synchronized void handleResendAccept(JSONObject json) throws JSONException{
    int slot = json.getInt("slot");
    if (coordinator != null && coordinator.pValuesCommander.containsKey(slot)) {
      GNS.getLogger().severe("Resend " + paxosID + "\tslot = " + slot);
      resendPendingProposal(coordinator.pValuesCommander.get(slot));
    }
  }

  /**
   * Send message p to replica {@code destID}.
   * @throws org.json.JSONException
   **/
  private void sendMessage(int destID, Packet p) {
    try
    {
      if (destID == nodeID) {
        handleIncomingMessage(p.toJSONObject(), p.packetType);
      }
      else {
        JSONObject json =  p.toJSONObject();
        paxosManager.sendMessage(destID, json, paxosID);
      }
    } catch (JSONException e) {
      e.printStackTrace();
      GNS.getLogger().severe(paxosID + "\t" + "JSON Exception. " + e.getMessage() + "P ");

    }
  }



  // TODO add log recovery methods here


  private int getInitialCoordinatorReplica() {

//    int nodeProduct = 1;
//    for (int x: nodeIDs) {
//      nodeProduct =  nodeProduct*x;
//    }

    String paxosID1 = paxosID;
    int index = paxosID.lastIndexOf("-");
    if (index > 0) paxosID1 = paxosID.substring(0, index);

    Random r = new Random(paxosID1.hashCode());

    ArrayList<Integer> x1  = new ArrayList<Integer>();
    for (int x: nodeIDs)
      x1.add(x);
    Collections.sort(x1);
    Collections.shuffle(x1, r);
    for (int x: x1) {
      return x;
    }
    return  x1.get(0);
//    return  x1.get(count);
  }



  private int getNextCoordinatorReplica() {

//    int nodeProduct = 1;
//    for (int x: nodeIDs) {
//      nodeProduct =  nodeProduct*x;
//    }

    String paxosID1 = paxosID;
    int index = paxosID.lastIndexOf("-");
    if (index > 0) paxosID1 = paxosID.substring(0, index);

    Random r = new Random(paxosID1.hashCode());
    ArrayList<Integer> x1  = new ArrayList<Integer>();
    for (int x: nodeIDs)
      x1.add(x);
    Collections.sort(x1);
    Collections.shuffle(x1, r);
    for (int x: x1) {
      if (paxosManager.isNodeUp(x)) return x;
    }
    return  x1.get(0);
//    return  x1.get(count);
  }



  /**
   * Check if a given node is in this Paxos instance.
   * @param nodeID
   * @return true if node belongs to this paxos instance.
   */
  public synchronized  boolean isNodeInPaxosInstance(int nodeID) {
    for (int i = 0; i < nodeIDs.length; i++) if (nodeID == i) return true;
    return false;
  }

  /**
   * is this paxos instance stopped?
   * @return
   */
  public synchronized boolean isStopped() {
//    synchronized (stopLock) {
    return isStopped;
//    }
  }


  public synchronized StatePacket getState() {

    String dbState = paxosManager.clientRequestHandler.getState(paxosID);
    if (dbState == null) {
      GNS.getLogger().severe(paxosID + "\t" + nodeID + "\tError Exception Paxos state not logged because database state is null.");
      return null;
    }
    StatePacket packet = new StatePacket(acceptorBallot, slotNumber, dbState);
    return  packet;

  }

  @Override
  public void recoverCurrentBallotNumber(Ballot b) {
    if (b == null) return;
    if (acceptorBallot == null || b.compareTo(acceptorBallot) > 0) {
      GNS.getLogger().fine(paxosID + "\t" + nodeID +
              " Paxos Recovery: Ballot updated to " + b);
      acceptorBallot = b;
    }
  }

  @Override
  public void recoverSlotNumber(int slotNumber) {
    if (slotNumber > this.slotNumber) {
      // update garbage collection slot
      GNS.getLogger().fine(paxosID + "\t" + nodeID +
              " Paxos Recovery: slotNumber updated to " + slotNumber);
      this.slotNumber = slotNumber;
    }
  }

  @Override
  public void executeRecoveredDecisions() {
    // TODO logging related method
  }

  @Override
  public void recoverStop() {
    // TODO logging related method
  }

  @Override
  public void recoverDecision(ProposalPacket proposalPacket) {
    if (slotNumber <= proposalPacket.slot) {
      GNS.getLogger().fine(paxosID + "\t" + nodeID +
              " Paxos Recovery: added decision for slot " + proposalPacket.slot);
      replicaMessages.add(proposalPacket);
    }

  }

  @Override
  public void recoverPrepare(PreparePacket preparePacket) {
    if (preparePacket.ballot.compareTo(acceptorBallot) > 0) {
      acceptorBallot = preparePacket.ballot;
    }
  }

  @Override
  public void recoverAccept(AcceptPacket acceptPacket) {
    PValuePacket pValuePacket = acceptPacket.pValue;
    if (slotNumber <= pValuePacket.proposal.slot) {

      int compare = pValuePacket.ballot.compareTo(acceptorBallot);
      if (compare > 0) {
        acceptorBallot = pValuePacket.ballot;
        replicaMessages.add(pValuePacket);
//        pValuesAccepted.put(pValuePacket.proposal.slot, pValuePacket);

      } else if (compare == 0) {
        replicaMessages.add(pValuePacket);
//        pValuesAccepted.put(pValuePacket.proposal.slot, pValuePacket);
      }
      // else ignore if pvalue packet is less than current acceptor ballot
    }
  }

  public String getPaxosID() {
    return paxosID;
  }

  public synchronized Ballot getAcceptorBallot() {
    return acceptorBallot;
  }

  /**
   * Received request from client, so propose it to the current coordinator.
   *
   * @param json
   * @throws JSONException
   */
  public synchronized void handleRequest(JSONObject json) throws JSONException{
//        GNS.getLogger().fine(" Send request to coordinator .... ");
    // send proposal to coordinator.
//        maxSlot = -1;
//        ProposalPacket propPacket = new ProposalPacket(-1,
//                req, PaxosPacketType.PROPOSAL);
//        Ballot b = getBallot();
//    int coordinatorID = -1;
    Ballot temp = null;
//    try{
//      acceptorLock.lock();
    if (acceptorBallot!=null) {
      temp = new Ballot(acceptorBallot.ballotNumber, acceptorBallot.coordinatorID);
//        coordinatorID = acceptorBallot.getCoordinatorID();
    }
//    } finally {
//      acceptorLock.unlock();
//    }

    json.put(ProposalPacket.SLOT, 0);
    json.put(PaxosPacketType.ptype, PaxosPacketType.PROPOSAL);
    json.put(paxosManager.PAXOS_ID, paxosID);

    if (temp != null && paxosManager.isNodeUp(temp.coordinatorID)) {
      paxosManager.sendMessage(temp.coordinatorID,json, paxosID);
      if (StartNameServer.debugMode) GNS.getLogger().fine(paxosID + "\t" + nodeID +
              " Send proposal packet. Coordinator =  " + temp.coordinatorID + " Packet = " + json);
    } else {
      // if coordinator has failed, resend to other nodes who may be coordinators
      UpdateBallotTask ballotTask = new UpdateBallotTask(paxosManager, this, temp, json);
      paxosManager.executorService.scheduleAtFixedRate(ballotTask, 0, paxosManager.RESEND_PENDING_MSG_INTERVAL_MILLIS,
              TimeUnit.MILLISECONDS);
    }
  }

  public synchronized boolean isAcceptorBallotUpdated(Ballot ballot) {
    return acceptorBallot.compareTo(ballot) > 0;
  }


  private void handleDecision(ProposalPacket prop, boolean recovery) throws JSONException {
    boolean stop = handleDecisionActual(prop, recovery);
    if (stop) {
      synchronized (paxosManager.paxosInstances) {
        PaxosReplicaInterface r = paxosManager.paxosInstances.get(paxosManager.getPaxosKeyFromPaxosID(paxosID));
        if (r == null) return;
        if (r.getPaxosID().equals(paxosID)) {
          if (StartNameServer.debugMode) GNS.getLogger().fine("Paxos instance removed " + paxosID  + "\tReq ");
          paxosManager.paxosInstances.remove(paxosManager.getPaxosKeyFromPaxosID(paxosID));
  //          r.logFullResponseAfterStop();
        } else {
          if (StartNameServer.debugMode) GNS.getLogger().fine("Paxos instance already removed " + paxosID);
        }
      }
    }
  }
  /**
   * Handle decision response from coordinator.
   * @param prop
   * @throws JSONException
   */
  private synchronized boolean handleDecisionActual(ProposalPacket prop, boolean recovery) throws JSONException {

    if (prop != null) {
      if (prop.slot < slotNumber) return false;
      replicaMessages.add(prop);

//      if (prop.slot%10 == 1)
//            PaxosLogger.logDecision(paxosID,prop);
      if (StartNameServer.debugMode) GNS.getLogger().fine(paxosID + "\t" + nodeID + " Decision recvd at slot = " + prop.slot + " req = " + prop.req);
      // Add decision to slot. Union operation in Figure 1.
//      PaxosManager.addToPaxosLog(json, paxosID);
//      if (decisions.size() > 20 || pValuesAccepted.size() > 20) {
//
//        GNS.getLogger().finer(paxosID + "\t" +nodeID + " Decision size = " + decisions.size() +
//                " Pvalue size = " + pValuesAccepted.size());
//      }
//      decisions.put(prop.slot, prop.req);
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

//    GNS.getLogger().info("Paxos ID: " + paxosID + " Slot " + slotNumber + " Tasks: " + paxosManager.executorService.getTaskCount()
//            + " " + (paxosManager.executorService.getTaskCount() - paxosManager.executorService.getCompletedTaskCount()));

    // sort through all and see if next executable exists
    boolean stop = false;
    while (true) {
      boolean flag = true;
      int count = 0;
      for (Object o: replicaMessages) {
        if (o.getClass().equals(ProposalPacket.class)) {
          count += 1;
          ProposalPacket packet = (ProposalPacket) o;
          if (packet.slot == slotNumber) {
            if (packet.req.isStopRequest()) stop = true;
            perform(packet.req, slotNumber, recovery);

            flag = false;
            slotNumber++;
          }
        }
      }
      if (flag) break;
      if (count == 1) break;
    }
    // scan and delete any pValues or decisions before this slot
    for (int i = replicaMessages.size() - 1; i >= 0; i--) {
      if (replicaMessages.get(i).getClass().equals(ProposalPacket.class)) {
        if (((ProposalPacket) replicaMessages.get(i)).slot < slotNumber)
          replicaMessages.remove(i);
      }
      else if (replicaMessages.get(i).getClass().equals(PValuePacket.class)) {
        if (((PValuePacket) replicaMessages.get(i)).proposal.slot < slotNumber)
          replicaMessages.remove(i);
      }
    }

    if (replicaMessages.size() > 0 && acceptorBallot.coordinatorID != nodeID) {
      // inform coordinator of my current sequence number
      SynchronizeReplyPacket syncReply = new SynchronizeReplyPacket(nodeID, slotNumber, new ArrayList<Integer>(), false);
      sendMessage(acceptorBallot.coordinatorID, syncReply);
    }
    return stop;
  }


  /**
   * Apply the decision locally, e.g. write to disk.
   * @param req
   * @throws JSONException
   */
  private void perform(RequestPacket req, int slotNumber, boolean recovery) throws JSONException{
    if (StartNameServer.debugMode) GNS.getLogger().info("\tPAXOS-PERFORM\t" + paxosID + "\t" + nodeID + "\t" + slotNumber  + "\t" + req.value);
    if (req.value.equals(NO_OP) ) {

      GNS.getLogger().fine(paxosID + "\t" +nodeID + " " + NO_OP + " decided in slot = " + slotNumber);
      return;
    }

//        boolean stop = paxosManager.isStopCommand(req.value);
    if (req.isStopRequest()) {
//      synchronized (stopLock) {
      isStopped = true;
      if (coordinator != null) { //
        coordinator = null;
      }

//      }
      GNS.getLogger().fine(" Logging paxos stop " + req);
      paxosManager.paxosLogger.logPaxosStop(paxosID);
    }
    paxosManager.handleDecision(paxosID, req, recovery);

//        GNS.getLogger().info("\tPAXOS-COMMIT\t" + paxosID + "\t" + nodeID + "\t" + slotNumber  + "\t" + req.value);

  }

  private synchronized void handleSendState(SendCurrentStatePacket2 statePkt) throws JSONException{
    if (slotNumber < statePkt.slotNumber) {
      paxosManager.clientRequestHandler.updateState(paxosID, statePkt.dbState);
      slotNumber = statePkt.slotNumber;
      handleDecision(null, false);
    }
    if (acceptorBallot.compareTo(statePkt.currentBallot) < 0) {
      acceptorBallot = statePkt.currentBallot;
    }
  }

  /**
   *
   * @return
   */
  private int getNextProposalSlotNumber() {
    if (coordinator != null) {
//    synchronized (proposalNumberLock) {
      if (coordinator.stopCommandProposed) return -1;
      coordinator.nextProposalSlotNumber += 1;
      return  coordinator.nextProposalSlotNumber - 1;
    }
    return -1;
//    }
  }

  private void updateNextProposalSlotNumber(int slotNumber) {
    if (coordinator != null) {
//    synchronized (proposalNumberLock) {
      if (coordinator.stopCommandProposed) return;
      if (slotNumber > coordinator.nextProposalSlotNumber) coordinator.nextProposalSlotNumber = slotNumber;
    }
//    }
  }

  /**
   *
   * @param p
   * @throws JSONException
   */
  private synchronized void handleProposal(ProposalPacket p) throws JSONException {

    if (StartNameServer.debugMode) GNS.getLogger().fine(paxosID + "C\t" +nodeID + " Coordinator handling proposal: " +
            p.toJSONObject().toString());

    if (coordinator != null) {
//      for (ProposalStateAtCoordinator p1: coordinator.pValuesCommander.values()) {
//        if (p1.pValuePacket.proposal.req.value.equals(p.req.value)) return;
//      }
//      Ballot b = null;
      if (StartNameServer.debugMode) GNS.getLogger().fine(paxosID + "C\t" +nodeID + " Coordinator active.");
      p.slot = getNextProposalSlotNumber();
      if (StartNameServer.debugMode) GNS.getLogger().fine(paxosID + "C\t" +nodeID + " Slot = " + p.slot);
      if (p.slot == -1) return;
      if (StartNameServer.debugMode) GNS.getLogger().fine(paxosID + "C\t" + nodeID + "C " + "Coordinator starting commander at slot = " + p.slot);
      initCommander(new PValuePacket(coordinator.coordinatorBallot, p));
    }
    else {
      GNS.getLogger().fine(paxosID + "C\t" +nodeID +" Coordinator not active.");
    }

  }

  /**
   * ballot adopted, coordinator elected, get proposed values accepted.
   * @throws JSONException
   */
  private void handleAdoptedNew() throws JSONException {
    if (coordinator == null) return;

//    if (StartNameServer.debugMode)

    GNS.getLogger().info(paxosID + "C\t" +nodeID + "C Ballot adopted. Handling proposals. Paxos ID = " + paxosID +
            " Num Nodes = " + nodeIDs.length);

    // this is executed after locking "scoutLock"

    // propose pValue received
    ArrayList<Integer> pValueSlots = new ArrayList<Integer>(coordinator.pValuesScout.keySet());
    Collections.sort(pValueSlots);
    int stopCommandSlot = -1;

    ArrayList<PValuePacket> selectPValues = new ArrayList<PValuePacket>();
    for (int slot: pValueSlots) {
      if (coordinator.pValuesScout.get(slot).proposal.req.isStopRequest()) {
        // check if it is to be executed
        boolean checkOutput = isStopCommandToBeProposed(slot);
        if (checkOutput) {
          // propose stop command
//                    proposals.put(slot, pValuesScout.get(slot).proposal);
          selectPValues.add(new PValuePacket(coordinator.ballotScout, coordinator.pValuesScout.get(slot).proposal));
          // propose stop command
          stopCommandSlot = slot;
          break;
        }
        else {
          // remove pvalue at slot, so that no-op is proposed
          coordinator.pValuesScout.remove(slot);
        }
      }
      else { // if (!r.isDecisionComplete(proposals.get(slot)))
        GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C " +
                "PValue proposal for slot: " + slot);
//                proposals.put(slot, pValuesScout.get(slot).proposal);
        selectPValues.add(new PValuePacket(coordinator.ballotScout, coordinator.pValuesScout.get(slot).proposal));
      }
    }
    GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C " +"Total PValue proposals : " + selectPValues.size());

    // propose no-ops in empty slots
    int maxSlotNumberAtReplica = getMaxSlotNumberAcrossNodes();
    GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C " +
            "Max slot number at replicas = "  + maxSlotNumberAtReplica);

    int maxPValueSlot = -1;
    for (int x: coordinator.pValuesScout.keySet()) {
      if (x > maxPValueSlot) {
        maxPValueSlot = x;
      }
    }
    GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C " + "Received pValues for slots  = " +
            coordinator.pValuesScout.keySet() + " max slot = " + maxPValueSlot);

    if (stopCommandSlot != -1 && stopCommandSlot < maxPValueSlot) maxPValueSlot = stopCommandSlot;

    ArrayList<Integer> noopSlots = new ArrayList<Integer>();
    // propose no-ops in slots for which no pValue is decided.
    for (int slot = maxSlotNumberAtReplica; slot <= maxPValueSlot; slot++) {
      if (!coordinator.pValuesScout.containsKey(slot)) {
        // propose a no-op command for it
        noopSlots.add(slot);
      }
    }
    // clear the entries in pValuesScout.
    coordinator.pValuesScout = null;
    coordinator.nodeSlotNumbers = null;

    int temp = maxSlotNumberAtReplica;
    if (maxPValueSlot + 1 > temp) temp = maxPValueSlot + 1;
    updateNextProposalSlotNumber(temp);
    GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C Next proposal slot number is: " + temp);
//    synchronized (coordinatorBallotLock) {
    coordinator.coordinatorBallot = coordinator.ballotScout;
//    activeCoordinator = true;
    coordinator.pValuesCommander.clear();
//    }

    GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C " + " Now starting commanders for pvalues ...");
    for (PValuePacket p: selectPValues) {
      initCommander(p);
    }

    GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C " + " Now proposing no-ops for missing slots ...");
    for (int x: noopSlots) {
      proposeNoopInSlot(x, coordinator.coordinatorBallot);
    }

  }


  /**
   * On a STOP command in pValuesScout, check whether to execute it or not
   * @param stopSlot the slot at which stop command is executed.
   * @return false if the stop command is not to be executed,
   * true if coordinator should propose the stop command.
   */
  private boolean isStopCommandToBeProposed(int stopSlot) {
    Ballot stopBallot = coordinator.pValuesScout.get(stopSlot).ballot;
    for (int x:coordinator.pValuesScout.keySet()) {
      if (x > stopSlot) {
        // check whether this ballot is higher than ballot of stop slot
        Ballot xBallot = coordinator.pValuesScout.get(x).ballot;
        // the ballot at this slot is higher than ballot at "stopSlot",
        // therefore "stopSlot" would not have been succesful.
        if(xBallot.compareTo(stopBallot) > 0) return false;
      }
    }
    return true;
  }

  private int getMaxSlotNumberAcrossNodes() {
    int max = 0;
    for (int x: coordinator.nodeSlotNumbers) {
//      GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C\tNode = " + x + "\tSlot = " + nodeAndSlotNumbers.get(x));
      if (x > max) max = x;
    }
    return max;
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
    ProposalStateAtCoordinator propState = new ProposalStateAtCoordinator(this, pValue, nodeIDs.length);

    coordinator.pValuesCommander.put(pValue.proposal.slot, propState);
    paxosManager.addToActiveProposals(propState);
//    paxosManager.executorService.scheduleAtFixedRate(new CheckAcceptMessageTask(this,propState, 10),
//            PaxosManager.RESEND_PENDING_MSG_INTERVAL_MILLIS, PaxosManager.RESEND_PENDING_MSG_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
    if (StartNameServer.debugMode) GNS.getLogger().fine(paxosID + "C\t" + nodeID + "C initialized commander values. Slot = " + pValue.proposal.slot);

    int minSlot = -1;
//    synchronized (minSlotLock) {
//    minSlot = coordinator.minSlotNumberAcrossNodes;
//            if (r.nextDouble() <= 0.001) System.out.println("nodes and slot numbers: " + nodeAndSlotNumbers);
//    }
    if (pValue.proposal.req.isStopRequest()) {
//      synchronized (proposalNumberLock){
      coordinator.stopCommandProposed = true;
      GNS.getLogger().info(paxosID + "C\t" +nodeID + "C" + " stop command proposed. slot = " + pValue.proposal.slot);
//      }
    }
    AcceptPacket accept = new AcceptPacket(this.nodeID, pValue, PaxosPacketType.ACCEPT, minSlot);

    if (StartNameServer.debugMode) GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C" + " Starting commander.");

//    try {
    JSONObject jsonObject = accept.toJSONObject();
//    GNS.getLogger().fine("This is json = " + jsonObject.toString());
//    jsonObject.put(PaxosManager.PAXOS_ID, paxosID);
    paxosManager.sendMessage(nodeIDs, jsonObject, paxosID, nodeID);
    sendMessage(nodeID, accept);
//    PaxosManager.sendMessage(nodeIDs,jsonObject,paxosID);

    // FULLRESPONSE
//    if (StartNameServer.experimentMode) GNS.getLogger().severe("\t" + paxosID + "\t" + pValue.proposal.slot + "\t"
// + pValue.proposal.req.isStopRequest() + "\tRequestProposed" );


//    if (StartNameServer.experimentMode)
//      GNS.getLogger().severe("\t" + paxosID + "\t" + pValue.proposal.slot + "\t"
// + pValue.proposal.req.isStopRequest() + "\tRequestProposed" );
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
  private synchronized void handleSyncReplyPacket(SynchronizeReplyPacket replyPacket) {
//        GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C here...");
//    updateNodeAndSlotNumbers(replyPacket);
    if (replyPacket.maxDecisionSlot < slotNumber) { // IMP: max decision slot is in fact slotNumber.
      String dbState = paxosManager.clientRequestHandler.getState(paxosID);
//      SendCurrentStatePacket2 statePkt = new SendCurrentStatePacket2(nodeID, acceptorBallot, slotNumber, dbState,
//              null, PaxosPacketType.SEND_STATE);
      sendMessage(replyPacket.nodeID, null);
    }
//    if (replyPacket.missingSlotNumbers != null) {
////      ArrayList<ProposalPacket> missingPackets = new ArrayList<ProposalPacket>();
////      boolean allMissingPacketsInMemory = false;
//      for (int x: replyPacket.missingSlotNumbers) {
//        if (decisions.containsKey(x)) {
////                    GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C " +
////                            "Sending decision for slot = " + x + " to node " + replyPacket.nodeID);
//          RequestPacket decision = decisions.get(x);
//          if (decision == null) continue;
//          ProposalPacket packet = new ProposalPacket(x, decision, PaxosPacketType.DECISION, 0);
////          missingPackets.add(packet);
//          sendMessage(replyPacket.nodeID, packet);
//        }
//        else {
//          // TODO read from db and send state to node.
////          GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C " +
////                  "Missing decision not found at coordinator for slot = " + x + " Decision: " + decisions.keySet());
//        }
//      }
//    }
//    if (replyPacket.flag) {
//      // resend decisions after max decision slot
//      for (int x: decisions.keySet()) {
//        if (x > replyPacket.maxDecisionSlot) {
//          RequestPacket decision = decisions.get(x);
//          if (decision == null) continue;
//          ProposalPacket packet = new ProposalPacket(x, decision, PaxosPacketType.DECISION, 0);
//          sendMessage(replyPacket.nodeID, packet);
//        }
//      }
//    }
  }

  /**
   * handle response of accept message from an acceptor.
   * @param accept
   * @throws JSONException
   */
  private synchronized LoggingCommand handleAcceptMessageReply(AcceptReplyPacket accept) throws JSONException{
    // resend missing requests
//        updateNodeAndSlotNumbers(accept.nodeID, accept.slotNumberAtReplica);

    int slot = accept.slotNumber;
    Ballot b = accept.ballot;
    int senderNode = accept.nodeID;
//        int deletelsot = accept.commitSlot;
//        updateNodeAndSlotNumbers(senderNode, deletelsot);
    if (StartNameServer.debugMode) GNS.getLogger().fine(paxosID + "C\t" + nodeID + "C Accept-Reply\tsender\t" + accept.nodeID + " slot\t" + slot);

    if (coordinator == null) return null;

    ProposalStateAtCoordinator stateAtCoordinator = coordinator.pValuesCommander.get(slot);

    if (stateAtCoordinator == null) {
//            GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C " +
//                    "Commander not found for slot =  " + slot + ". Either decision complete or ballot preempted.");
      return null;
    }
//        if (stateAtCoordinator.pValuePacket.proposal.req.equals(accept.pValue.proposal.req)) {
    if (b.compareTo(stateAtCoordinator.pValuePacket.ballot) <= 0) {
      // case: replica accepted proposed value
//                GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C ballot OK. ");
      stateAtCoordinator.addNode(senderNode);
//                GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C commander accept count: " +
//                        stateAtCoordinator.getNodesResponded() + " nodes "+ nodeIDs.size());
      return sendDecisionForSlot(slot, stateAtCoordinator);
    }
    else if (b.compareTo(stateAtCoordinator.pValuePacket.ballot) > 0){
      if (b.compareTo(acceptorBallot) > 0) acceptorBallot = new Ballot(b.ballotNumber, b.coordinatorID);
      // case: ballot preempted
      stateAtCoordinator = coordinator.pValuesCommander.remove(slot);

      if (stateAtCoordinator != null) {
        paxosManager.removeFromActiveProposals(stateAtCoordinator);
        //                    waitForCommander.remove(slot);
        GNS.getLogger().severe(paxosID + "C\t" + nodeID + "C " + "higher ballot recvd. current ballot preempted.");
//        try {
//          acceptorLock.lock();
//          synchronized (coordinatorBallotLock) {
        if (coordinator.coordinatorBallot.compareTo(stateAtCoordinator.pValuePacket.ballot) == 0) {
          coordinator = null;
//          activeCoordinator = false;
//          coordinatorBallot = b;
//          pValuesCommander.clear();
        }
        if (getNextCoordinatorReplica() == nodeID) initScout();
//          }

//        if (stateAtCoordinator.pValuePacket.ballot.compareTo(acceptorBallot) > 0) {
//          acceptorBallot = new Ballot(b.ballotNumber, b.coordinatorID);
////                        PaxosLogger.logCurrentBallot(paxosID, acceptorBallot);
//        }
//        }   finally {
//          acceptorLock.unlock();
//        }

      }
    }
    return null;
//            else {
//                GNS.getLogger().severe(paxosID + "C\t" +nodeID + "C " +
//                        "Lower ballot recvd. ERROR THIS CASE SHOULD NOT HAPPEN! Recevd ballot = "
// + accept.pValue.ballot
//                        + " Proposed ballot = " + stateAtCoordinator.pValuePacket.ballot);
    // this case should not arise
//            }
//        }
  }


  private LoggingCommand sendDecisionForSlot(int slot, ProposalStateAtCoordinator stateAtCoordinator) {
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

    if (stateAtCoordinator.isMajorityReached() == false) {return null;}

    // remove object from pValuesCommander
    stateAtCoordinator = coordinator.pValuesCommander.remove(slot);
    if (stateAtCoordinator == null) return null;

    paxosManager.removeFromActiveProposals(stateAtCoordinator);

    // if object deleted return


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
    int minSlot = -1;
//    synchronized (minSlotLock) {
//    minSlot = minSlotNumberAcrossNodes;
//            if (r.nextDouble() <= 0.001) System.out.println("nodes and slot numbers: " + nodeAndSlotNumbers);
//    }

    try {
      stateAtCoordinator.pValuePacket.proposal.gcSlot = minSlot;
      JSONObject jsonObject = stateAtCoordinator.pValuePacket.proposal.toJSONObject();
//      jsonObject.put(PaxosManager.PAXOS_ID, paxosID);
      paxosManager.sendMessage(nodeIDs, jsonObject, paxosID, nodeID);

      return new LoggingCommand(paxosID, jsonObject, LoggingCommand.LOG_AND_EXECUTE);

//      PaxosLogger.logMessage(new LoggingCommand(paxosID, jsonObject, LoggingCommand.LOG_AND_EXECUTE));

//      sendMessage(nodeID, stateAtCoordinator.pValuePacket.proposal);
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
    return null;
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

//    synchronized (proposalNumberLock) {

    if (coordinator == null) coordinator = new PaxosCoordinator();
    else if (coordinator.stopCommandProposed) return; // in case stop command is proposed, make it null.
//    }

//    try{
//      scoutLock.lock();
//      try{
//        acceptorLock.lock();
////            synchronized (acceptorBallot) {
//        synchronized (coordinatorBallotLock) {

//          if (activeCoordinator) { // if coordinator is currently activeCoordinator, dont choose new ballot.
////                        if (StartNameServer.debugMode) GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C Init scout skipped. already active." );
//            return;
//          }
    // scout will now choose a ballot
    if (coordinator.ballotScout != null && coordinator.ballotScout.compareTo(acceptorBallot) > 0) {
      coordinator.ballotScout = new Ballot(coordinator.ballotScout.ballotNumber + 1, nodeID);
    }
    else {
      coordinator.ballotScout = new Ballot(acceptorBallot.ballotNumber + 1, nodeID);
    }
//        }
//            updateBallot(b);
//      }finally {
//        acceptorLock.unlock();
//      }

    GNS.getLogger().severe(paxosID + "C\t" + nodeID + "C Coordinator proposing new ballot: " + coordinator.ballotScout);

//            scoutingInProgress = true;
    coordinator.waitForScout = new ArrayList<Integer>();
    coordinator.pValuesScout = new HashMap<Integer,PValuePacket>();
    coordinator.nodeSlotNumbers = new int[nodeIDs.length];

//    }finally {
//      scoutLock.unlock();
//    }

    // create prepare packet
    PreparePacket prepare = new PreparePacket(nodeID, 0, coordinator.ballotScout, PaxosPacketType.PREPARE);
//		for (int i = 1; i <= Network.N; i++) {
    for (int i: nodeIDs) {
      prepare.receiverID = i;
//            GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C sending prepare msg to " +i );
      sendMessage(i, prepare);
    }

    int numberRetry = 50; //(int) (FailureDetection.timeoutIntervalMillis /
    // PaxosManager.RESEND_PENDING_MSG_INTERVAL_MILLIS);
    CheckPrepareMessageTask task = new CheckPrepareMessageTask(this,prepare, coordinator.ballotScout, numberRetry);

    paxosManager.executorService.scheduleAtFixedRate(task,PaxosManager.RESEND_PENDING_MSG_INTERVAL_MILLIS,
            PaxosManager.RESEND_PENDING_MSG_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);

  }

  /**
   * Returns true if there is a need to resend the prepare message again, otherwise false.
   * @param proposedBallot
   * @param prepare
   * @return
   */
  public synchronized boolean resendPrepare(Ballot proposedBallot, PreparePacket prepare) {

    if (coordinator == null || proposedBallot.equals(coordinator.ballotScout) == false || coordinator.waitForScout == null)
    {
      GNS.getLogger().severe(paxosID + "\t" + nodeID + "C\t No need to resend PreparePacket. Ballot adopted = " +
              proposedBallot);
      return false;
    }

//    try {
//      scoutLock.lock();

    // what if majority have failed.
    int nodesUp = 0;
    for (int x: nodeIDs)
      if (paxosManager.isNodeUp(x)) nodesUp++;
    if (nodesUp*2 < nodeIDs.length) return false; // more than half node down, give up resending prepare
    // TODO: what do we do when more nodes come back up?
    boolean resend = false;
    for (int x: nodeIDs) {
      if (coordinator.waitForScout.contains(x) == false && paxosManager.isNodeUp(x)) {
        sendMessage(x, prepare); // send message to the node if it is up and has not responded.
        resend = true;
        GNS.getLogger().fine(paxosID + "\t" + nodeID + "C\t Ballot = " + proposedBallot + " resend to node " + x);
      }
    }
    return resend;

//    } finally {
//      scoutLock.unlock();
//    }

  }

  @Override
  public Set<Integer> getNodeIDs() {
    HashSet<Integer> nodeSet = new HashSet<Integer>();
    for (int x: nodeIDs) nodeSet.add(x);
    return nodeSet;
  }


  @Override
  public Map<Integer, PValuePacket> getPValuesAccepted() {
    // TODO not implemented
    return null;
  }

  @Override
  public Map<Integer, RequestPacket> getCommittedRequests() {
    // TODO not implemented
    return null;
  }

  @Override
  public int getSlotNumber() {
    return slotNumber;
  }

//  /**
//   * Updates slotNumber for nodeID in data structure nodeAndSlotNumbers
//   * @param nodeID
//   * @param slotNumber
//   */
//  private void updateNodeAndSlotNumbers(int nodeID, int slotNumber) {
////        synchronized (nodeAndSlotNumbers) {
//    if (coordinator == null) return;
//    for (int i = 0; i < nodeIDs.length; i++) {
//      if(nodeIDs[i] == nodeID && coordinator.nodeSlotNumbers[i] < slotNumber)
//        coordinator.nodeSlotNumbers[i] = slotNumber;
//    }
////    if (!nodeAndSlotNumbers.containsKey(nodeID) ||
////            slotNumber > nodeAndSlotNumbers.get(nodeID)) {
//////            GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C Slot number for " + nodeID
//////                    + " updated to node " + slotNumber);
////      nodeAndSlotNumbers.put(nodeID, slotNumber);
////    }
////        }
//  }

//  /**
//   * Updates nodeAndSlotNumbers based on replyPacket
//   * @param
//   */
//  private void updateNodeAndSlotNumbers(SynchronizeReplyPacket replyPacket) {
////        if (replyPacket.maxDecisionSlot ==  -1) {
////            return;
////        }
//
//    int minSlot = Integer.MAX_VALUE;
//    if (replyPacket.missingSlotNumbers != null) {
//      for (int x: replyPacket.missingSlotNumbers) {
//        if (minSlot > x) minSlot = x;
//      }
//    }
//    else {
//      minSlot = replyPacket.maxDecisionSlot;
//    }
//
//    if (minSlot == Integer.MAX_VALUE) return;
//
//    minSlot += 1;
//
//
//
//    for (int i = 0; i < nodeIDs.length; i++) {
//      if(nodeIDs[i] == replyPacket.nodeID && coordinator.nodeSlotNumbers[i] < minSlot)
//        coordinator.nodeSlotNumbers[i] = minSlot;
//    }
//
////    int node = replyPacket.nodeID;
////        synchronized (nodeAndSlotNumbers) {
////    if (!nodeAndSlotNumbers.containsKey(node) || minSlot > nodeAndSlotNumbers.get(node)) {
//////            GNS.getLogger().fine(paxosID + "C\t" +node + "C node = " + node
//////                    + " Update: slot number = " + minSlot);
////      nodeAndSlotNumbers.put(node, minSlot);
////    }
////    synchronized (minSlotLock){
////    minSlotNumberAcrossNodes = getMinSlotNumberAcrossNodes();
////    }
//
//  }

  private synchronized void handlePrepareMessageReply(PreparePacket prepare) throws JSONException{

//    updateNodeAndSlotNumbers(prepare.receiverID, prepare.slotNumber);

//    try{scoutLock.lock();
//        synchronized (scoutingInProgress) {
    if (coordinator == null) return;
    if (coordinator.waitForScout == null) {
//                GNS.getLogger().fine(paxosID + "C\t" + nodeID + "C not getting ballot accepted now.");
      return;
    }
    for (int i = 0; i < nodeIDs.length; i++) {
      if (nodeIDs[i] == prepare.receiverID) {
        coordinator.nodeSlotNumbers[i] = prepare.slotNumber;
      }
    }
//            GNS.getLogger().fine(paxosID + "C\t" +nodeID
//                    + "C received Prepare-Reply Msg "+prepare.toJSONObject().toString());
    if (coordinator.waitForScout.contains(prepare.receiverID)) {
//                GNS.getLogger().fine(paxosID + "C\t" +nodeID
//                        + "C already received prepare response from " + prepare.receiverID);
      return;
    }

    if (prepare.ballot.compareTo(coordinator.ballotScout) < 0) {
//                GNS.getLogger().fine(paxosID + "C\t" +nodeID
//                        + "C received response for a earlier scout. Ballot scout = " + ballotScout
//                        + " Message ballot = " + prepare.ballot);
      return;
    }
    else if (prepare.ballot.compareTo(coordinator.ballotScout) > 0) {
                GNS.getLogger().severe(paxosID + "C\t" + nodeID + "C Ballot pre-empted. ");
      if (getNextCoordinatorReplica() == nodeID) initScout();
      else coordinator = null;
//                scoutingInProgress = false;
//                handlePreempted(prepare.ballot);
    }
    else {
//                GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C Ballot accepted " +
//                        ballotScout + " by node " + prepare.receiverID);
      for(PValuePacket pVal: prepare.accepted.values()) {
        int slot = pVal.proposal.slot;
        if (!coordinator.pValuesScout.containsKey(slot) ||
                prepare.ballot.compareTo(coordinator.pValuesScout.get(slot).ballot) > 0)  {
          coordinator.pValuesScout.put(slot, pVal);
//                        GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C " +
//                                "pValue from node " + prepare.receiverID + "added to list: " + pval.toJSONObject());
        }
      }

      coordinator.waitForScout.add(prepare.receiverID);
//                GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C " +
//                        "Scout prepare msg count: " + waitForScout.size());
      if (coordinator.waitForScout.size() > nodeIDs.length / 2) { // activeCoordinator == false &&
        // case: ballot accepted by majority, cooridinator elected
//                    GNS.getLogger().fine(paxosID + "C\t" +nodeID + "C " +
//                            "Scout received majority votes for ballot. " + ballotScout + "Votes = "
//                            + waitForScout.size() + " Nodes =  " + nodeIDs.size());
        coordinator.waitForScout = null;
//                    scoutingInProgress = false;
        handleAdoptedNew();
      }
    }

  }

//  private Ballot resendPrepareMessage(int nodeID) {
//    // is this necessary?
//  }

  @Override
  public void checkInitScout() {

  }

  /**
   * check whether coordinator is UP.
   */
  public synchronized void checkCoordinatorFailure() {
    if (paxosManager.isNodeUp(acceptorBallot.coordinatorID) == false && getNextCoordinatorReplica() == nodeID) {
      GNS.getLogger().severe(paxosID + "C\t" +nodeID +"C coordinator failed. " + acceptorBallot.coordinatorID);
      initScout();
    }
  }

  /**
   * message reporting node failure.
   * @param packet
   */
  private synchronized void handleNodeStatusUpdate(FailureDetectionPacket packet) {
//    updateSelectNodes();
    if (packet.status) return; // node is up.
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
//    try{
//      acceptorLock.lock();

    if (acceptorBallot != null ) {
//        synchronized (coordinatorBallotLock) {
      coordinatorID = acceptorBallot.getCoordinatorID();
//        }
    }
//    } finally {
//      acceptorLock.unlock();
//    }
    if (packet.responderNodeID == coordinatorID // current coordinator has failed.
            && getNextCoordinatorReplica() == nodeID) { // I am next coordinator

      GNS.getLogger().warning(paxosID + "C\t" + nodeID + " Coordinator failed\t" + packet.responderNodeID +
              " Propose new ballot.");

//      if (StartNameServer.debugMode)
      GNS.getLogger().severe(paxosID + "C\t" +nodeID +"C coordinator has failed " + coordinatorID);
      if (StartNameServer.debugMode)
        GNS.getLogger().fine(paxosID + "C\t" +nodeID + " current ballot coordinator failed " + coordinatorID);
      // I should try to get a new ballot accepted.
      initScout();
    }
  }

//  /**
//   * Minimum slot number in the list: nodeAndSlotNumbers.
//   * @return
//   */
//  private int getMinSlotNumberAcrossNodes( ) {
////    if (nodeAndSlotNumbers.size() < nodeIDs.size()) return -1;
//    int minSlot = -1;
//    for (int nodeID: nodeAndSlotNumbers.keySet()) {
//      if (FailureDetection.isNodeUp(nodeID) == false) continue;
//      if (minSlot == -1 || minSlot > nodeAndSlotNumbers.get(nodeID))  {
//        minSlot = nodeAndSlotNumbers.get(nodeID);
//      }
//    }
//    return minSlot;
//  }


  /**
   * New ballot proposed by a replica, handle the proposed ballot.
   * @param packet
   * @throws JSONException
   */
  private synchronized LoggingCommand handlePrepare(JSONObject incomingJson, PreparePacket packet) throws JSONException {
    //        PaxosLogger.logCurrentBallot(paxosID, packet.ballot);
    //
    if (StartNameServer.debugMode) GNS.getLogger().fine(paxosID + "\t" +nodeID + " Acceptor received PreparePacket: " + packet);
//        Ballot b1 = null;
//    try{
//      acceptorLock.lock();
//        synchronized (acceptorBallot) {
//                b1 = new Ballot(acceptorBallot.getBallotNumber(), acceptorBallot.getCoordinatorID());
    if (acceptorBallot == null || packet.ballot.compareTo(acceptorBallot) > 0) {
      acceptorBallot = packet.ballot;
    }

//            sendMessage(packet.coordinatorID,
//                    packet.getPrepareReplyPacket(acceptorBallot, pValuesAccepted, slot));
    // Log this message and send reply to coordinator
    HashMap<Integer, PValuePacket> pValuesAccepted = new HashMap<Integer, PValuePacket>();
    for (int i = 0; i < replicaMessages.size(); i++) {
      if (replicaMessages.get(i).getClass().equals(PValuePacket.class)) {
        PValuePacket pValuePacket = (PValuePacket) replicaMessages.get(i);
        pValuesAccepted.put(pValuePacket.proposal.slot, pValuePacket);
      }
    }

    Packet p = packet.getPrepareReplyPacket(acceptorBallot, nodeID, pValuesAccepted, slotNumber);
//
//      PaxosManager.addToPaxosLog(incomingJson,paxosID);
//      sendMessage(packet.coordinatorID,p);

    return new LoggingCommand(paxosID, incomingJson, LoggingCommand.LOG_AND_SEND_MSG,
            packet.coordinatorID, p.toJSONObject());

//            b1 = new Ballot(acceptorBallot.getBallotNumber(), acceptorBallot.getCoordinatorID());
//        }
//    }finally {
//      acceptorLock.unlock();
//    }

//    if (StartNameServer.debugMode) GNS.getLogger().fine(paxosID + "\t" +"Reached here.");

  }


//  AcceptReplyPacket acceptReply;

  /**
   * Handle accept messages from coordinator
   * @param accept
   */
  private synchronized LoggingCommand handleAccept(JSONObject incomingJson, AcceptPacket accept) {
    if (StartNameServer.debugMode) GNS.getLogger().fine(paxosID + "\t" + nodeID + "\tAcceptorslot\t" + accept.pValue.proposal.slot);

//            PaxosLogger.logPValue(paxosID, accept.pValue);

//    Ballot b1 = null;
//      try {
//        acceptorLock.lock();
    AcceptReplyPacket acceptReply;
    if (accept.pValue.ballot.compareTo(acceptorBallot) == 0) { // accept the pvalue
      boolean add = true;
      for (int i = 0; i < replicaMessages.size(); i++) {
        if (replicaMessages.get(i).getClass().equals(PValuePacket.class)) {
          PValuePacket pValuePacket = (PValuePacket) replicaMessages.get(i);
          if (pValuePacket.proposal.slot == accept.pValue.proposal.slot) {
            replicaMessages.set(i, accept.pValue);
            add = false;
            break;
          }
        }
      }
      if (add) {
        replicaMessages.add(accept.pValue);
      }
//      pValuesAccepted.put(accept.pValue.proposal.slot, accept.pValue);
//                    b1 = acceptorBallot;
      acceptReply = new AcceptReplyPacket(nodeID, acceptorBallot, accept.pValue.proposal.slot);
//      if (acceptReply == null || acceptReply.ballot.equals(accept.pValue.ballot) == false)
//        acceptReply = new AcceptReplyPacket(nodeID, acceptorBallot, accept.pValue.proposal.slot);
//      else {
//        acceptReply.slotNumber = accept.pValue.proposal.slot;
//      }
//                    AcceptReplyPacket acceptReply = new AcceptReplyPacket(nodeID, b1,accept.pValue.proposal.slot);
//            AcceptPacket p = accept.getAcceptReplyPacket(nodeID, b1);
//                    sendMessage(accept.nodeID,acceptReply);

    } else if (accept.pValue.ballot.compareTo(acceptorBallot) > 0) {  // accept the pvalue and the ballot

      acceptorBallot = new Ballot(accept.pValue.ballot.ballotNumber, accept.pValue.ballot.coordinatorID);
//      b1 = acceptorBallot;
//                    PaxosLogger.logCurrentBallot(paxosID, acceptorBallot);
      boolean add = true;
      for (int i = 0; i < replicaMessages.size(); i++) {
        if (replicaMessages.get(i).getClass().equals(PValuePacket.class)) {
          PValuePacket pValuePacket = (PValuePacket) replicaMessages.get(i);
          if (pValuePacket.proposal.slot == accept.pValue.proposal.slot) {
            replicaMessages.set(i, accept.pValue);
            add = false;
            break;
          }
        }
      }
      if (add) {
        replicaMessages.add(accept.pValue);
      }
//      pValuesAccepted.put(accept.pValue.proposal.slot, accept.pValue);
      acceptReply = new AcceptReplyPacket(nodeID, acceptorBallot, accept.pValue.proposal.slot);
//            AcceptPacket p = accept.getAcceptReplyPacket(nodeID, b1);
//                    sendMessage(accept.nodeID,acceptReply);
    } else { // accept.pValue.ballot.compareTo(acceptorBallot) < 0)
      // do not accept, reply with own acceptor ballot
//      b1 = new Ballot(acceptorBallot.ballotNumber, acceptorBallot.coordinatorID);
      acceptReply = new AcceptReplyPacket(nodeID, acceptorBallot,accept.pValue.proposal.slot);
//            AcceptPacket p = accept.getAcceptReplyPacket(nodeID, b1);
//                    sendMessage(accept.nodeID,acceptReply);
    }
    // new: log and send message.

//        PaxosManager.addToPaxosLog(incomingJson,paxosID);
//        sendMessage(accept.nodeID, acceptReply);

    try {
      return new LoggingCommand(paxosID, incomingJson, LoggingCommand.LOG_AND_SEND_MSG, accept.nodeID,
              acceptReply.toJSONObject());
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return null;
  }

//  /**
//   * handle sync request message from a coordinator,
//   * return the current missing slot numbers and maximum slot number for which decision is received
//   * @param packet
//   */
//  private void handleSyncRequest(SynchronizePacket packet) {
//    SynchronizeReplyPacket synchronizeReplyPacket = getSyncReply(true);
//    GNS.getLogger().fine(paxosID + "\t" +nodeID + " Handling Sync Request: " + synchronizeReplyPacket);
//    sendMessage(packet.nodeID, synchronizeReplyPacket);
//  }


//  /**
//   * calculate the current missing slot numbers and maximum slot number for which decision is received
//   * @return a SynchronizeReplyPacket to be sent to coordinator
//   */
//  private SynchronizeReplyPacket getSyncReply(boolean flag) {
//    // TODO implement this
//    int maxDecision = -1;
//    for (Object o: decisions) {
//      if (o.getClass().equals(ProposalPacket.class)) {
//        if (((ProposalPacket)o).slot > maxDecision) maxDecision = ((ProposalPacket)o).slot;
//      }
//    }
//
//    ArrayList<Integer> missingSlotNumbers = new ArrayList<Integer>();
//    for (int i = slotNumber; i <= maxDecision; i++) {
//      if (!decisions.containsKey(i)) missingSlotNumbers.add(i);
//    }
//
//    return new SynchronizeReplyPacket(nodeID, maxDecision, missingSlotNumbers, flag);
//  }

  public synchronized boolean resendPendingProposal(ProposalStateAtCoordinator state) {
    // TODO we are assuming majority replicas are up.

    if (coordinator == null ||
            coordinator.pValuesCommander.containsKey(state.pValuePacket.proposal.slot) == false) return false;
//    try {
//      acceptorLock.lock();
    if (state.pValuePacket.ballot.compareTo(acceptorBallot) != 0) return false;
    if (StartNameServer.experimentMode) GNS.getLogger().info("\tResendingMessage\t" + paxosID + "\t" +
              state.pValuePacket.proposal.slot + "\t" + acceptorBallot + "\t");

//    }finally {
//      acceptorLock.unlock();
//    }

    int minSlot = -1; // not used for GC in this paxos implementation.
//    synchronized (minSlotLock) {
//    minSlot = minSlotNumberAcrossNodes;
//    }

    for (int node: nodeIDs) {
      // ACCEPT-REPLY received from node?
      if (!state.hasNode(node)) {
        AcceptPacket accept = new AcceptPacket(nodeID, state.pValuePacket, PaxosPacketType.ACCEPT, minSlot);
//        GNS.getLogger().severe("\tResendingMessage\t" + state.paxosID + "\t" +
//                state.pValuePacket.proposal.slot + "\t" + acceptorBallot + "\t" + node);
        sendMessage(node, accept);
      }
    }
    return true;

  }

  @Override
  public synchronized void removePendingProposals() {
    if (coordinator != null) {
      for (ProposalStateAtCoordinator p: coordinator.pValuesCommander.values()) paxosManager.removeFromActiveProposals(p);
      coordinator = null;
    }
  }


  public static void main(String[] args) {
    SizeOf.skipStaticField(true); //java.sizeOf will not compute static fields
    SizeOf.skipFinalField(false); //java.sizeOf will not compute final fields
    SizeOf.skipFlyweightObject(false); //java.sizeOf will not compute well-known flyweight objects

    int nodeCount = 10;
//    PaxosReplicaNew paxosReplicaNew = new PaxosReplicaNew("", 0, new HashSet<Integer>());
//    printSize(paxosReplicaNew);
//    paxosReplicaNew.version = 10;
//    printSize(paxosReplicaNew);
//    paxosReplicaNew.nodeIDs = new short[nodeCount];
//    printSize(paxosReplicaNew);
//    paxosReplicaNew.slotNumber = 100;
//    printSize(paxosReplicaNew);
//    paxosReplicaNew.replicaMessages = new ArrayList<Object>(4);
//    printSize(paxosReplicaNew);
//    paxosReplicaNew.acceptorBallot = new Ballot(10, 4);
//    printSize(paxosReplicaNew);


//    System.out.println();
//    PaxosCoordinator coordinator1 = new PaxosCoordinator();
//    coordinator1.coordinatorBallot = new Ballot(4,6);
//    printSize(coordinator1);
//    paxosReplicaNew.coordinator = coordinator1;
//    printSize(paxosReplicaNew);

    ArrayList<PaxosReplicaNew> paxosList = new ArrayList<PaxosReplicaNew>();
    for (int i = 0; i < 5000000;  i++) {
      Set<Integer> nodeIDs = new HashSet<Integer>();
      for (int j = 0; j < 10; j++) {
        nodeIDs.add(j);
      }
      paxosList.add(new PaxosReplicaNew(Util.randomString(5), 0, nodeIDs, null));
    }
    OutputMemoryUse.outputMemoryUse("Before");
    System.gc();
    OutputMemoryUse.outputMemoryUse("After");
    System.out.println("All paxos objects allocated.");
    try {
      Thread.sleep(10000);
      OutputMemoryUse.outputMemoryUse("10 sec After");
      Thread.sleep(1000000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  static void printSize(Object paxosReplicaNew){
    System.out.println("Size: " + SizeOf.deepSizeOf(paxosReplicaNew)); //this will print the object size in bytes
  }
}

///**
// *
// */
//class UpdateBallotTask2 extends TimerTask {
//
//  PaxosReplicaNew replica;
//
//  Ballot ballot;
//
//  int count = 0;
//
//  ArrayList<Integer> nodes;
//
//  JSONObject json;
//
//  public UpdateBallotTask2(PaxosReplicaNew replica, Ballot ballot, JSONObject json) {
//    this.replica = replica;
//    this.ballot = ballot;
//    String paxosID1 = PaxosManager.getPaxosKeyFromPaxosID(replica.getPaxosID()); //paxosID.split("-")[0];
//    Random r = new Random(paxosID1.hashCode());
//    nodes = new ArrayList<Integer>();
//    for (int x: replica.nodeIDs)
//
//      Collections.sort(nodes);
//    Collections.shuffle(nodes, r);
//    this.json = json;
//  }
//
//  @Override
//  public void run() {
//    if (count == nodes.size()) {
//      throw new RuntimeException();
//    }
//    PaxosReplicaNew pr = PaxosManager.paxosInstances.get(PaxosManager.getPaxosKeyFromPaxosID(replica.getPaxosID()));
//    // todo update this to use paxos replica only
//    if (pr == null || pr.getPaxosID().equals(replica.getPaxosID()) == false) {
//      throw new RuntimeException();
//    }
//    if (pr.isAcceptorBallotUpdated(ballot)) {
//      throw new RuntimeException();
//    }
//    int node = -1;
//    while(node == -1) {
//      if (count < nodes.size() && PaxosManager.isNodeUp(nodes.get(count))) {
//        node = nodes.get(count);
//      }
//      count += 1;
//    }
//    if (node > 0) {
//      PaxosManager.sendMessage(node, json);
//    }
//  }
//}



//class CheckAcceptMessageTask extends TimerTask {
//
//  int numRetry;
//  int count = 0;
//  PaxosReplicaInterface replica;
//  ProposalStateAtCoordinator proposalState;
//
//  public CheckAcceptMessageTask(PaxosReplicaInterface replica, ProposalStateAtCoordinator proposalState, int numRetry) {
//    this.numRetry = numRetry;
//    this.replica = replica;
//    this.proposalState = proposalState;
//  }
//
//  @Override
//  public void run() {
//    try {
//      if (replica.isStopped()) throw  new GnsRuntimeException();
//
//      boolean sendAgain = replica.resendPendingProposal(proposalState);
//      if (sendAgain == false || count == numRetry) throw  new GnsRuntimeException();
//    } catch (Exception e) {
//      if (e.getClass().equals(GnsRuntimeException.class)) {
//        throw new RuntimeException();
//      }
//      GNS.getLogger().severe("Exception in CheckPrepareMessageTask: " + e.getMessage());
//      e.printStackTrace();
//    }
//  }
//}