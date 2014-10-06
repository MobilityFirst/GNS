package edu.umass.cs.gns.paxos;

import edu.umass.cs.gns.paxos.paxospacket.PValuePacket;
import edu.umass.cs.gns.paxos.paxospacket.RequestPacket;
import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Analyzes paxos logs to find any problems with paxos logs collected during a test run of the system.
 * User: abhigyan
 * Date: 11/22/13
 * Time: 4:31 PM
 * @param <NodeIDType>
 */
public class PaxosLogAnalyzer<NodeIDType> {
  // could consider renaming class

  /**
   * Folder containing paxos logs collected at each node
   */
  String paxosLogFolder;

  /**
   * Number of name servers whose nodeIDs are 0, 1, ..., (numNS - 1).
   */
  int numNS;

  /**
   * Log messages at all nodes for all paxos instances. A nested HashMap data structure is used to store the messages.
   */
  HashMap<NodeIDType, ConcurrentHashMap<String, PaxosReplicaInterface>> paxosAllNS;

  /**
   * Constructor
   *
   * @param paxosLogFolder
   * @param numNS
   */
  public PaxosLogAnalyzer(String paxosLogFolder, int numNS) {
    this.paxosLogFolder = paxosLogFolder;
    this.numNS = numNS;
    paxosAllNS = new HashMap<NodeIDType, ConcurrentHashMap<String, PaxosReplicaInterface>>();
  }

  // FIX ME: THIS CODE ASSUMES NODES WILL BE NAMED 0 - N.
  /**
   * Reads all data from logs and reconstructs all paxos instances from every node
   */
  public void readLogData() {

//    PaxosLogger.setGnsRunning(false);
    for (int x = 0; x < numNS; x++) {
      NodeIDType nodeID = (NodeIDType) Integer.toString(x);

      String nodeLogFolder = getNodeLogFolder(nodeID);
      PaxosLogger logger = new PaxosLogger(nodeLogFolder, nodeID, false);
//      PaxosLogger.setLoggerParameters(nodeLogFolder);
      long t0 = System.currentTimeMillis();
      ConcurrentHashMap<String, PaxosReplicaInterface> paxosInstances = logger.readAllPaxosLogs();
      paxosAllNS.put(nodeID, paxosInstances);
      System.out.println("Read log: Node " + nodeID + "\tPaxosIDs: " + paxosInstances.size()
              + "\t" + (System.currentTimeMillis() - t0) / 1000 + " sec");
    }
//    for (Integer nodeID: paxosAllNS.keySet()) {
//      System.out.println("Node " + nodeID);
//      for (String paxosID: paxosAllNS.toString(nodeID).keySet()) {
//        System.out.println("\t" + paxosID + "\t" + paxosAllNS.toString(nodeID).toString(paxosID).getPValuesAccepted().keySet().size()
//                + "\t" + paxosAllNS.toString(nodeID).toString(paxosID).getCommittedRequests().keySet().size());
//      }
//      System.out.println();
//    }

  }

  /**
   *
   * @param nodeID
   * @return name of folder where paxos logs for {@code nodeID} are stored.
   */
  private String getNodeLogFolder(NodeIDType nodeID) {
    return paxosLogFolder + "/log_" + nodeID.toString();
  }

  public void outputPaxosStats(String outputFile, String failedNodesFile) {

    HashSet<Integer> failedNodes = readFailedNodes(failedNodesFile);
    HashMap<String, Set<NodeIDType>> paxosNodeIDs = new HashMap<String, Set<NodeIDType>>();
    for (NodeIDType nodeID : paxosAllNS.keySet()) {
      for (String paxosID : paxosAllNS.get(nodeID).keySet()) {
        if (paxosNodeIDs.containsKey(paxosID) == false) {
          paxosNodeIDs.put(paxosID, paxosAllNS.get(nodeID).get(paxosID).getNodeIDs());
        }
      }
    }

    System.out.println("instance count\t" + paxosNodeIDs.size());

    ArrayList<PaxosSlotStats> missingRequestMessages = new ArrayList<PaxosSlotStats>();

    HashMap<String, Integer> paxosIDSlots = new HashMap<String, Integer>();
    for (String paxosID : paxosNodeIDs.keySet()) {

      int paxosGroupSize = paxosNodeIDs.get(paxosID).size();
      int defaultCoordinator = -1; // TODO fix this.
//      int defaultCoordinator = PaxosReplica.getDefaultCoordinatorReplica(paxosID, paxosNodeIDs.toString(paxosID));

      // check which slots have any activity, print deficit info.
      // paxosID slot numreplica commitdeficit acceptdeficit defaultcoordinator
      int slotCount = -1;

      for (NodeIDType nodeID : paxosNodeIDs.get(paxosID)) {
        PaxosReplicaInterface replica = paxosAllNS.get(nodeID).get(paxosID);
        if (replica == null) {
          continue;
        }
        for (Object object : replica.getPValuesAccepted().keySet()) {
          int slot = (int) object;
          if (slot > slotCount) {
            slotCount = slot;
          }
        }
      }
      slotCount += 1;
      paxosIDSlots.put(paxosID, slotCount);

      boolean lastRequestStop = false; // true if request proposed in last episode is a stop request
      for (NodeIDType nodeID : paxosNodeIDs.get(paxosID)) {
        PaxosReplicaInterface replica = paxosAllNS.get(nodeID).get(paxosID);
        if (replica == null) {
          continue;
        }
        if (replica.getCommittedRequests().containsKey(slotCount - 1)
                && replica.getPValuesAccepted().containsKey(slotCount - 1)) {
          RequestPacket requestPacket = (RequestPacket) replica.getCommittedRequests().get(slotCount - 1);
          PValuePacket pValuePacket = (PValuePacket) replica.getPValuesAccepted().get(slotCount - 1);
          if (requestPacket.isStopRequest() && pValuePacket.proposal.req.isStopRequest()) {
            lastRequestStop = true;
            break;
          }
        }

//        if ((replica.getCommittedRequests().containsKey(slotCount - 1)
//                && replica.getCommittedRequests().get(slotCount - 1).isStopRequest())
//                || (replica.getPValuesAccepted().containsKey(slotCount - 1)
//                && replica.getPValuesAccepted().get(slotCount - 1).proposal.req.isStopRequest())) {
//          lastRequestStop = true;
//          break;
//        }
      }

      for (int slot = 0; slot < slotCount; slot++) {

        int acceptCount = 0;
        int commitCount = 0;
        int acceptCountFailed = 0;
        int commitCountFailed = 0;
        for (NodeIDType nodeID : paxosNodeIDs.get(paxosID)) {
          if (failedNodes.contains(nodeID)) {
            acceptCountFailed += 1;
            commitCountFailed += 1;

          }
          PaxosReplicaInterface replica = paxosAllNS.get(nodeID).get(paxosID);
          if (replica == null) {
            // paxos instance not created at node:
//            System.err.println("PaxosID\t" + paxosID + "\tnot created at node\t" + nodeID);
            continue;
          }

          if (replica.getPValuesAccepted().containsKey(slot)) {
            acceptCount += 1;
            if (failedNodes.contains(nodeID) == false) {
              acceptCountFailed += 1;
            }
          }

          if (replica.getCommittedRequests().containsKey(slot)) {
            commitCount += 1;
            if (failedNodes.contains(nodeID) == false) {
              commitCountFailed += 1;
            }
          }
        }
        if (acceptCount < paxosGroupSize || commitCount < paxosGroupSize) {
          // missing messages
          boolean isStop = false;
          if (slot == slotCount - 1 && lastRequestStop) {
            isStop = true;
          }
          missingRequestMessages.add(new PaxosSlotStats(paxosID, paxosGroupSize, defaultCoordinator, slotCount,
                  slot, (paxosGroupSize - acceptCount), (paxosGroupSize - commitCount),
                  (paxosGroupSize - acceptCountFailed), (paxosGroupSize - commitCountFailed), isStop));
//          System.err.println("Missing\t" + paxosID + "\t" + slot + "\t" + paxosGroupSize + "\t" +
//                  (paxosGroupSize - acceptCount) + "\t"  + (paxosGroupSize - commitCount));
        } else {       // all messages found.
//          System.out.println("Complete\t" + paxosID + "\t" + slot + "\t" + paxosGroupSize + "\t" +
//                  (paxosGroupSize - acceptCount) + "\t"  + (paxosGroupSize - commitCount));

        }
      }

    }

    System.out.println(
            "paxosIDSlots count\t" + paxosIDSlots.size());

    File f = new File(outputFile);
    String parentFolder = f.getParent();
    if (parentFolder
            != null) { // create parent folder if necessary
      f = new File(f.getParent());
      if (!f.exists()) { // if folder does not exist for parent
        f.mkdirs(); // create parent folder
      }
    }

    try {
      FileWriter fw = new FileWriter(outputFile);
      for (PaxosSlotStats message : missingRequestMessages) {
        fw.write(message.toString());
      }
      fw.close();
    } catch (IOException e) {
      e.printStackTrace();
    }

    System.out.println(
            "Output File: " + outputFile);
  }

  public HashSet<Integer> readFailedNodes(String failedNodesFile) {
    try {
      HashSet<Integer> failedNodes = new HashSet<Integer>();
      File f = new File(failedNodesFile);
      if (f.isFile()) {
        BufferedReader br = new BufferedReader(new FileReader(f));
        String line = br.readLine();
        if (line == null) {
          return failedNodes;
        }
        String[] tokens = line.split("\\s+");
        for (String x : tokens) {
          failedNodes.add(Integer.parseInt(x));
        }
      }
      return failedNodes;
    } catch (IOException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      return new HashSet<Integer>();
    }
  }

  public static void main(String[] args) {
    String paxosLogFolder = args[0]; //"/Users/abhigyan/Documents/workspace/GNS2/local/paxoslog/";
    String outputFile = args[1]; //"/Users/abhigyan/Documents/workspace/GNS2/local/paxosMissingMessages";
    int numNS = Integer.parseInt(args[2]);

    String failedNodesFile = args[3];

    PaxosLogAnalyzer logAnalyzer = new PaxosLogAnalyzer(paxosLogFolder, numNS);

    long t0 = System.currentTimeMillis();

    logAnalyzer.readLogData();

    System.out.println("\nLog reading complete. Duration = " + (System.currentTimeMillis() - t0) / 1000 + " sec\n");
    t0 = System.currentTimeMillis();

    logAnalyzer.outputPaxosStats(outputFile, failedNodesFile);

    System.out.println("\nLog analysis complete. Duration = " + (System.currentTimeMillis() - t0) / 1000 + " sec\n");
    System.exit(2);

  }
}

class PaxosSlotStats {

  private String paxosID;

  private int numReplica;

  private int defaultCoordinator;

  private int slotCount;

  private int slot;

  private int missingAccept;

  private int missingCommit;

  private int missingAcceptExcludingFailed;

  private int missingCommitExcludingFailed;

  private int isStop;

  public PaxosSlotStats(String paxosID, int numReplica, int defaultCoordinator, int slotCount, int slot,
          int missingAccept, int missingCommit, int missingAcceptExcludingFailed,
          int missingCommitExcludingFailed, boolean isStop) {
    this.paxosID = paxosID;
    this.numReplica = numReplica;
    this.defaultCoordinator = defaultCoordinator;
    this.slotCount = slotCount;
    this.slot = slot;
    this.missingAccept = missingAccept;
    this.missingCommit = missingCommit;
    this.missingAcceptExcludingFailed = missingAcceptExcludingFailed;
    this.missingCommitExcludingFailed = missingCommitExcludingFailed;
    if (isStop) {
      this.isStop = 1;
    } else {
      this.isStop = 0;
    }
  }

  @Override
  public String toString() {
    return paxosID + "\t" + numReplica + "\t" + defaultCoordinator + "\t" + slotCount + "\t" + slot + "\t"
            + missingAccept + "\t" + missingCommit + "\t" + missingAcceptExcludingFailed + "\t"
            + missingCommitExcludingFailed + "\t" + isStop + "\n";
  }

}
