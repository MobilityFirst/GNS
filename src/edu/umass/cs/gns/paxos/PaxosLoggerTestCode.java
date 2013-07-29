package edu.umass.cs.gns.paxos;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartNameServer;
import edu.umass.cs.gns.packet.paxospacket.PValuePacket;
import edu.umass.cs.gns.packet.paxospacket.PaxosPacketType;
import edu.umass.cs.gns.packet.paxospacket.ProposalPacket;
import edu.umass.cs.gns.packet.paxospacket.RequestPacket;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created with IntelliJ IDEA.
 * User: abhigyan
 * Date: 7/10/13
 * Time: 5:46 PM
 * To change this template use File | Settings | File Templates.
 */
public class PaxosLoggerTestCode {

    String paxosID = "myPaxosID";

    int nodes = 5;
    Set<Integer> nodeIDs;

    int clientID = 0;
    int myID = 0;
    int slotNumber = 0;
    int garbageCollectionSlot = 0;
    Ballot currentBallot = new Ballot(0, 0);

    public void writeReplicaLogs() {

        System.out.println(" Writing Replica Logs .. ");
        nodeIDs = new HashSet<Integer>();
        for (int i = 0; i < nodes; i++) {
            nodeIDs.add(i);
        }
        // log start
        PaxosLogger.logPaxosStart(paxosID, nodeIDs);

        // update ballot
        currentBallot = new Ballot(currentBallot.ballotNumber + 1, nodes - 1);
        PaxosLogger.logCurrentBallot(paxosID, currentBallot);

        PValuePacket pValuePacket = generatePValuePacket(clientID, "00000", 0, currentBallot);
        PaxosLogger.logPValue(paxosID, pValuePacket);

        pValuePacket = generatePValuePacket(clientID, "11111", 1, currentBallot);
        PaxosLogger.logPValue(paxosID, pValuePacket);

        // send decision for slot 0
        ProposalPacket proposalPacket = generateProposalPacket(0,"00000",0);
        PaxosLogger.logDecision(paxosID, proposalPacket);

        // send decision for slot 1
        proposalPacket = generateProposalPacket(1,"11111",1);
        PaxosLogger.logDecision(paxosID, proposalPacket);

        PaxosLogger.logCurrentSlotNumber(paxosID, 2);

        PaxosLogger.logGarbageCollectionSlot(paxosID, 1);
    }

    public void compareRecoveredReplica(PaxosReplica replica) {

        System.out.println("\nComparing recovered replica with test data ...\n");
        int slotNumber = 2;
        int garbageCollectionSlot = 0;
        Ballot b = currentBallot;
        int[] decisionSlots = {1};
        int[] pValueSlots = {1};


        if (replica.getSlotNumber() == slotNumber) {
            System.out.println("Slot number match: " + slotNumber);
        }
        else {
            System.out.println("ERROR: Slot number does not match: " + slotNumber);
        }

        if (replica.getGarbageCollectionSlot() == garbageCollectionSlot) {
            System.out.println("Garbage collection slot match: " + garbageCollectionSlot);
        }
        else {
            System.out.println("ERROR: Garbage collection slot does not match: " + garbageCollectionSlot);
        }

        if (replica.getAcceptorBallot().compareTo(b) == 0) {
            System.out.println("Ballot matches: " + b);
        }
        else {
            System.out.println("ERROR: Ballot does not match: " + b);
        }

        ConcurrentHashMap<Integer, RequestPacket> replicaDecisions = replica.getDecisions();
        if (decisionSlots.length == replicaDecisions.size()) {
            System.out.println("Number of decision slots matches: " + decisionSlots.length);
        }
        else  {
            System.out.println("ERROR:Number of decision slots does not match: " + decisionSlots.length);
        }


        for (int x: decisionSlots) {
            if (replicaDecisions.containsKey(x)) {
                System.out.println("Replica has decision for slot = " + x +
                        " Decision = " + replicaDecisions.get(x).value);
            }
            else {
                System.out.println("ERROR: Replica does not have decision for slot = " + x);
            }
        }


        ConcurrentHashMap<Integer, PValuePacket> pValues = replica.getPValuesAccepted();
        if (pValueSlots.length == pValues.size()) {
            System.out.println("Number of pValue slots matches: " + pValueSlots.length);
        }
        else  {
            System.out.println("ERROR: Number of pValue slots does not match: " + pValueSlots.length);
        }

        for (int x: pValueSlots) {
            if (pValues.containsKey(x)) {
                System.out.println("Replica has pValue for slot = " + x  +
                        " pValue = " + pValues.get(x).proposal.req.value +
                        " Ballot = " + pValues.get(x).ballot);
            }
            else {
                System.out.println("ERROR: Replica does not have pValue for slot = " + x);
            }
        }

        System.out.println("Replica comparison complete");





    }

    public  void testStartStop() {
        int paxosInstances = 1;


        HashSet<Integer> nodeIDs = new HashSet<Integer>();
        for (int i = 0; i < nodes; i++) {
            nodeIDs.add(i);
        }

        for (int i = 0; i < paxosInstances; i++) {
            PaxosLogger.logPaxosStart(Integer.toString(i), nodeIDs);
        }
//        for (int i = 0; i < paxosInstances; i++) {
//            PaxosLogger.logPaxosStop(Integer.toString(i), 10);
//        }
    }



    private  ProposalPacket generateProposalPacket(int clientID, String value, int slotNumber) {
        RequestPacket req = new RequestPacket(clientID,value,PaxosPacketType.REQUEST);
        ProposalPacket proposalPacket = new ProposalPacket(slotNumber,req, PaxosPacketType.PROPOSAL);
        return proposalPacket;

    }

    private PValuePacket generatePValuePacket(int clientID, String value, int slotNumber, Ballot b) {
        RequestPacket req = new RequestPacket(clientID,value,PaxosPacketType.REQUEST);
        ProposalPacket proposalPacket = new ProposalPacket(slotNumber,req, PaxosPacketType.PROPOSAL);
        PValuePacket pValuePacket = new PValuePacket(b,proposalPacket);
        return pValuePacket;

    }


    public static void main(String[] args) {
        StartNameServer.debugMode = true;
        PaxosLogger.logFolder = "testLog";


//        String[] files = PaxosLogger.getSortedLogFileList();
//        for (String f: files) {
//            System.out.println(f);
//        }
        PaxosLogger.clearLogs();
        PaxosLogger.initializePaxosLogger();

        PaxosLoggerTestCode code = new PaxosLoggerTestCode();
        code.writeReplicaLogs();

//        testStartStop();
//        testDecision();

        ConcurrentHashMap<String, PaxosReplica> paxosInstances = PaxosLogger.recoverPaxosInstancesFromLogs();

        for (String x: paxosInstances.keySet()){
            code.compareRecoveredReplica(paxosInstances.get(x));
        }

        if (StartNameServer.debugMode) GNS.getLogger().fine(" Test Complete.");


    }

}

