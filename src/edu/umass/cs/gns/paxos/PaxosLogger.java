package edu.umass.cs.gns.paxos;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartNameServer;
import edu.umass.cs.gns.nameserver.NameServer;
import edu.umass.cs.gns.packet.paxospacket.*;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Logging class used to log paxos messages and state for all paxos instances.
 *
 * All logs are stored in folder {@code paxosLogFolder}. Log folder contains 3 types of files:
 *
 * (1) Paxos IDs file: This file is stored in {@code paxosIDsFile}. It logs two types of events: (1) a paxos instance is created
 * (2)a paxos instance is stopped. This is an infinite log.
 * On startup, this {@code paxosIDsFile} tells which paxos instances were active before the system stopped.
 *
 * (2) Paxos state: These files are stored in folder {@code logFolder}/{@code stateFolderSubdir}. It contains periodic
 * snapshots of complete state of all active paxos instance. The name of each file is formatted as follows:
 * 'paxosID_ballot_slotNumber'. the ballot is formatted as 'ballotNumber:coordinatorID'. The content of each file is
 * as follows. The first line contains an integer represented in string format. The integer tells the
 * the length of the paxos state contained in the file. Lines after the first line contain the state of the paxos instance.
 *
 * (3) Log message files: This files are stored '{@code logFolder}/{@code logFilePrefix}_X' where 'X' is a non-negative
 * integer. These files the paxos messages as as they are processed. Each file contains at most {@code MSG_MAX} messages.
 * After {@code MSG_MAX} are logged to file {@code logFilePrefix}_X, further message are logged to
 * {@code logFilePrefix}_(X+1).
 *
 * Messages to be logged in are queued. A logging thread checks the queue periodically,
 * logs messages, and then performs the action associated with every logging message.
 * Currently, we log three types of messages: PREPARE, ACCEPT, and DECISION.  For example, once the DECISION message
 * is logged, it is forwarded to the paxos instance for processing.
 *
 * TODO describe format of logs
 *
 * TODO describe recovery process
 *
 * TODO describe redundant log deletion process
 *
 * logs messages, and then sends off messages for processing.
 * User: abhigyan
 * Date: 7/24/13
 * Time: 8:16 AM
 */
public class PaxosLogger extends Thread {

  /**
   * lock object controlling access to {@code logCommands}
   */
  private static final Object logQueueLock = new ReentrantLock();
  /**
   * messages currently queued for logging
   */
  private static ArrayList<LoggingCommand> logCommands = new ArrayList<LoggingCommand>();
  /**
   * if {@code msgs} is empty (no new messages for logging), then the logging thread
   * will sleep for {@code SLEEP_INTERVAL_MS} before checking the {@code msgs} again.
   */
  private static int SLEEP_INTERVAL_MS = 1;
  /**
   * folder used to store paxos log
   */
  static String logFolder = null;
  /**
   * {@code FileWriter} object currently used for logging
   */
//  static FileWriter fileWriter;
  static String logFileName;
  /**
   * file number of log file currently used for logging
   */
  private static int logFileNumber = 0;
  /**
   * number of messages written to current log file
   */
  private static int msgCount = 0;
  /**
   * after writing {@code MSG_MAX} messages to a file, a new log file is used
   */
  private static int MSG_MAX = 10000;
  /**
   * name of file which stores
   */
  private static String paxosIDsFile = "paxosIDs";
  /**
   * names of  paxos log files start with following prefix
   */
  private static String logFilePrefix = "paxoslog_";
  /**
   * This sub-directory periodically stores most recent snapshot of all paxos instances
   */
  private static String stateFolderSubdir = "paxosState";
  private static String paxosStateFolder = null;
  /**
   * Lock object used to isolate access to writes to paxos IDs file
   */
  private static final ReentrantLock loggingLock = new ReentrantLock();

  // ADDED NODE + NameServer.nodeID so we can run multiple servers on a single machine
  private static String getLogFolderPath() {
    return logFolder + "/" + "NODE" + NameServer.nodeID;
  }
  // <state>
  // <last message>
  // <not logged>
  // get state
  //

  /**
   * This method is called on startup. It initializes paxos logs
   * and recovers state from existing logs when paxos starts up.
   * @return {@code ConcurrentHashMap} of {@code PaxosReplica} objects recovered from logs, {@code null} if no object exists, keys
   * of {@code ConcurrentHashMap} are paxos IDs
   */
  public static ConcurrentHashMap<String, PaxosReplica> initLogger() {

    if (logFolder == null) {
      System.out.println("Specify paxosLogFolder. paxosLogFolder can't be null.");
      System.exit(2);
    }

    // initialize folder
    paxosStateFolder = getLogFolderPath() + "/" + stateFolderSubdir;

    ConcurrentHashMap<String, PaxosReplica> replicas = null;

    createLogDirs();

    replicas = recoverPaxosInstancesFromLogs();

    logFileName = getNextFileName();
//    if (StartNameServer.debugMode) GNS.getLogger().fine(" Logger Initialized.");
//    try {
//
//    } catch (IOException e) {
//      e.printStackTrace();
//    }

    if (StartNameServer.debugMode) {
      GNS.getLogger().fine(" File Writer created.");
    }
    LoggingThread thread = new LoggingThread();
    thread.start();
    if (StartNameServer.debugMode) {
      GNS.getLogger().fine(" Thread started.");
    }

    return replicas;
  }

  /**
   * This method recovers paxos state from logs at system startup
   * @return {@code ConcurrentHashMap} of {@code PaxosReplica} objects recovered from logs, {@code null} if no object exists, keys
   * of {@code ConcurrentHashMap} are paxos IDs
   */
  private static ConcurrentHashMap<String, PaxosReplica> recoverPaxosInstancesFromLogs() {

    // step 1: recover list of paxos instances
    ConcurrentHashMap<String, PaxosReplica> paxosInstances = recoverListOfPaxosInstances();
    if (paxosInstances == null) {
      return null;
    }

    // step 2: recover most recent state logged for each paxos instance
    recoverMostRecentStateOfPaxosInstances(paxosInstances);

    // step 3: read paxos logs for messages received after the paxos state was logged
    recoverLogMessagesAfterLoggedState(paxosInstances);


    if (StartNameServer.debugMode) {
      GNS.getLogger().fine("Paxos Recovery: Complete.");
    }

    return paxosInstances;
  }

  /**
   * Recover list of paxos instances currently active.
   * @return
   */
  private static ConcurrentHashMap<String, PaxosReplica> recoverListOfPaxosInstances() {

    ConcurrentHashMap<String, PaxosReplica> paxosInstances = new ConcurrentHashMap<String, PaxosReplica>();
    // step 1: read paxosIDs active currently
    File f = new File(getPaxosIDsFile());

    if (!f.exists()) {
      if (StartNameServer.debugMode) {
        GNS.getLogger().fine("Paxos Recovery: " + getPaxosIDsFile() + " does not exist. "
                + "No further recovery possible.");
      }
      return null;
    }

    if (StartNameServer.debugMode) {
      GNS.getLogger().fine("Paxos Recovery: start reading paxos IDs file ...");
    }
    try {
      BufferedReader br = new BufferedReader(new FileReader(f));
      while (true) {
        String line = br.readLine();
        if (line == null) {
          break;
        }
        parseLine(paxosInstances, line);
      }
      br.close();
    } catch (IOException e) {
      e.printStackTrace();
    }

    if (StartNameServer.debugMode) {
      GNS.getLogger().fine("Paxos Recovery: completed reading paxos IDs file. "
              + "Number of Paxos IDs = " + paxosInstances.size());
    }
    appendToFile(getPaxosIDsFile(), "\n");
    return paxosInstances;
  }

  /**
   * Update the paxos instances with most recently logged state found in logs for each instance
   * @param paxosInstances
   */
  private static void recoverMostRecentStateOfPaxosInstances(ConcurrentHashMap<String, PaxosReplica> paxosInstances) {
    File f = new File(getPaxosStateFolder());
    if (f.exists() == false) {
      if (StartNameServer.debugMode) {
        GNS.getLogger().fine(" ");
      }
      return;
    }
    File[] files = f.listFiles();
    ConcurrentHashMap<String, PaxosStateFileName> mostRecentStateFile = new ConcurrentHashMap<String, PaxosStateFileName>();
    for (File f1 : files) {
      try {
        PaxosStateFileName fname = new PaxosStateFileName(f1.getName());
        if (!paxosInstances.containsKey(fname.paxosID)) {
          continue;
        }
        String state = getPaxosStateFromFile(f1);
        if (state == null) {
          continue;
        } else {
          fname.updateState(state);
        }
        if (mostRecentStateFile.containsKey(fname.paxosID) == false) {
          GNS.getLogger().fine(" Put state in file " + f1.getName() + " for paxos ID " + fname.paxosID);
          mostRecentStateFile.put(fname.paxosID, fname);
        } else if (fname.compareTo(mostRecentStateFile.get(fname.paxosID)) > 0) {
          GNS.getLogger().fine(" REPLACE: state in file " + f1.getName() + " for paxos ID " + fname.paxosID);
          mostRecentStateFile.put(fname.paxosID, fname);
        } else {
          GNS.getLogger().fine(" IGNORE: state in file " + f1.getName() + " for paxos ID " + fname.paxosID);
        }
      } catch (Exception e) {
        GNS.getLogger().fine(" ERROR Parsing log state file name: " + f1.getName());
      }
    }

    for (String x : paxosInstances.keySet()) {
      if (mostRecentStateFile.containsKey(x) == false) {
        GNS.getLogger().severe("ERROR: No state logged for paxos instance = " + x + "\tThis case should not happen.");
      } else {
        PaxosStateFileName state = mostRecentStateFile.get(x);
        paxosInstances.get(x).recoverCurrentBallotNumber(state.ballot);
        paxosInstances.get(x).recoverSlotNumber(state.slotNumber);
        PaxosManager.clientRequestHandler.updateState(x, state.state);
      }
    }
  }

  /**
   * Update the paxos instances with messages received after the paxos state was logged most recently for
   * each instance
   * @param paxosInstances
   */
  private static void recoverLogMessagesAfterLoggedState(ConcurrentHashMap<String, PaxosReplica> paxosInstances) {

    // get list of log files
    String[] fileList = getSortedLogFileList();

    if (StartNameServer.debugMode) {
      GNS.getLogger().fine("Paxos Recovery: number of log files found: " + fileList.length);
    }

    if (fileList.length > 0) {
      String fileName = fileList[fileList.length - 1];
      // logFileNumber is the highest log file number found in log folder
      logFileNumber = getLogFileNumber(fileName);
      // we will choose the current logFileNumber one more the highest logFileNumber in the log folder.
    }

    for (int i = 0; i < fileList.length; i++) {
      if (StartNameServer.debugMode) {
        GNS.getLogger().fine("Paxos Recovery: Now recovering log file: " + fileList[i]);
      }
      try {
        BufferedReader br = new BufferedReader(new FileReader(new File(getLogFolderPath() + "/" + fileList[i])));
        while (true) {
          String line = br.readLine();
          if (line == null) {
            break;
          }
          if (StartNameServer.debugMode) {
            GNS.getLogger().fine("Recovering line: " + line);
          }
          parseLine(paxosInstances, line);
        }
        br.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    // execute decisions
    for (String paxosID : paxosInstances.keySet()) {
      paxosInstances.get(paxosID).executeRecoveredDecisions();
    }
  }

  /**
   * Parse a single line from paxos logs and update {@code paxosInstances} accordingly.
   * @param paxosInstances set of paxos instances
   * @param line paxos log line
   */
  private static void parseLine(ConcurrentHashMap<String, PaxosReplica> paxosInstances, String line) {
    if (line.length() <= 1) {
      return;
    }
    PaxosLogMessage logMessage;
    try {
      // construct a log message object
      logMessage = new PaxosLogMessage(line);
      // handle update to paxos instances

      updatePaxosInstances(paxosInstances, logMessage);
      if (StartNameServer.debugMode) {
        GNS.getLogger().fine("Recovered log msg: " + line);
      }

    } catch (Exception e) {
      e.printStackTrace();
      if (StartNameServer.debugMode) {
        GNS.getLogger().fine("Exception in recovering log msg: " + line);
      }
      return;
    }

  }

  private static void updatePaxosInstances(ConcurrentHashMap<String, PaxosReplica> paxosInstances,
                                           PaxosLogMessage logMessage) throws JSONException {
    if (paxosInstances == null || logMessage == null) {
      if (StartNameServer.debugMode) {
        GNS.getLogger().fine("Ignored log msg:" + logMessage);
      }
      return;
    }

    switch (logMessage.getLogMessageType()) {
      case PaxosPacketType.START:
        parsePaxosStart(paxosInstances, logMessage.getPaxosID(), logMessage.getMessage());
        break;
      case PaxosPacketType.STOP:
        parsePaxosStop(paxosInstances, logMessage.getPaxosID(), logMessage.getMessage());
        break;
      case PaxosPacketType.DECISION:
        parseDecision(paxosInstances.get(logMessage.getPaxosID()), logMessage.getMessage());
        break;
//      case PaxosPacketType.PREPARE:
//        parsePValue(paxosInstances.get(logMessage.getPaxosID()), logMessage.getMessage());
//        break;
      case PaxosPacketType.ACCEPT:
        parseAccept(paxosInstances.get(logMessage.getPaxosID()), logMessage.getMessage());
        break;
      case PaxosPacketType.PREPARE:
        parsePrepare(paxosInstances.get(logMessage.getPaxosID()), logMessage.getMessage());
        break;

//      case BALLOT:
//        parseCurrentBallot(paxosInstances.get(logMessage.getPaxosID()), logMessage.getMessage());
//        break;
//      case SLOTNUMBER:
//        parseCurrentSlotNumber(paxosInstances.get(logMessage.getPaxosID()), logMessage.getMessage());
//        break;
//      case GARBAGESLOT:
//        parseGarbageCollectionSlot(paxosInstances.get(logMessage.getPaxosID()), logMessage.getMessage());
//        break;
    }

  }

  private static void createLogDirs() {
    File f = new File(getLogFolderPath());
    if (!f.exists()) { // paxos folder does not exist, then create dirs to store logs.
      f.mkdirs(); // create getLogFolderPath()
    }

    f = new File(paxosStateFolder);
    if (!f.exists()) {
      f.mkdirs(); // create paxosStateFolder
    }

  }

  /**
   * Clear all the paxos logs. Used for testing only.
   */
  static void clearLogs() {
    if (logFolder != null) {
      getLogFolderPath().length();
      File f = new File(getLogFolderPath());
      deleteDir(f);
      createLogDirs();// recreate log dirs if they do not exist.
    }

  }

  /**
   * Recursively deletes the given file
   * @param f
   */
  private static void deleteDir(File f) {
    if (f.exists() == false) {
      return;
    }
    if (f.isFile()) {
      f.delete();
      return;
    }

    File[] f1 = f.listFiles();
    for (File f2 : f1) {
      if (f2.isFile()) {
        f2.delete();
      } else if (f2.isDirectory()) {
        deleteDir(f2);
      }
    }
    f.delete();
  }

  /**
   * Logs to disk that this paxos instance is created.
   * @param paxosID
   * @param nodeIDs
   * @param initialState
   */
  static void logPaxosStart(String paxosID, Set<Integer> nodeIDs, StatePacket initialState) {
    if (StartNameServer.debugMode) {
      GNS.getLogger().fine(" Paxos ID = " + paxosID);
    }
    if (StartNameServer.debugMode) {
      GNS.getLogger().fine(" Node IDs = " + nodeIDs);
    }
    if (StartNameServer.debugMode) {
      GNS.getLogger().fine(" Initial state = " + initialState.state);
    }
    String paxosIDsFile1 = getPaxosIDsFile();
    if (paxosIDsFile1 != null) {
      synchronized (loggingLock) {
        // first log initial state
        logPaxosState(paxosID, initialState);
        // then append to paxos IDs
        String logString = getLogString(paxosID, PaxosPacketType.START, setIntegerToString(nodeIDs));
        appendToFile(paxosIDsFile1, logString);
      }
    }
  }

  /**
   * TODO add doc here
   *
   * @param filename
   * @param logString
   */
  private static void appendToFile(String filename, String logString) {
    try {
      FileWriter fw = new FileWriter(filename, true);
      fw.write(logString);
      fw.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static String setIntegerToString(Set<Integer> integerSet) {
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (int x : integerSet) {
      if (first) {
        sb.append(x);
        first = false;
      } else {
        sb.append(":" + x);
      }
    }
    return sb.toString();
  }

  private static Set<Integer> stringToSetInteger(String string) {
//    System.out.println(string);
    Set<Integer> integerSet = new HashSet<Integer>();
    String[] tokens = string.split(":");
    for (String s : tokens) {
      integerSet.add(Integer.parseInt(s));
    }
    return integerSet;
  }

  /**
   * Logs the string describing initial state of this paxos instance to disk
   * @param paxosID
   * @param initialState
   */
  private static void logInitialPaxosState(String paxosID, String initialState) {
    //
  }

  /**
   * TODO add doc here
   * @param paxosID
   */
  static void logPaxosStop(String paxosID) {

    String paxosIDsFile1 = getPaxosIDsFile();

    synchronized (loggingLock) {
      String logString = null;
//      try {
      logString = getLogString(paxosID, PaxosPacketType.STOP, Integer.toString(PaxosPacketType.STOP));
//      } catch (JSONException e) {
//        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//        return;
//      }
      appendToFile(paxosIDsFile1, logString);
    }
  }

  /**
   *
   * @param paxosID
   * @param packet
   */
  static void logPaxosState(String paxosID, StatePacket packet) {

    synchronized (loggingLock) {
      String name = getStateLogFileName(paxosID, packet);
      try {
        FileWriter fw = new FileWriter(name);
        if (packet.state.endsWith("\n")) {
          fw.write(Integer.toString(packet.state.length()));
          fw.write("\n");
          fw.write(packet.state);
        } else {
          fw.write(Integer.toString(packet.state.length() + 1));
          fw.write("\n");
          fw.write(packet.state);
          fw.write("\n"); // new line
        }
        // log length of state

        // log state (could be multiple lines)
        fw.close();
      } catch (IOException e) {
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }
    }
  }

  /**
   *
   * @param f
   * @return
   */
  private static String getPaxosStateFromFile(File f) {

    try {
      StringBuilder sb = new StringBuilder();
      BufferedReader br = new BufferedReader(new FileReader(f));
      String x = br.readLine();
      int size = Integer.parseInt(x);
      if (StartNameServer.debugMode) {
        GNS.getLogger().fine(" Size = " + size);
      }
      int lc = 0; // line count
      while (true) {
        String s = br.readLine();
        if (s == null) {
          break;
        }
        sb.append(s);
        sb.append("\n");
        lc++;
      }
      br.close();
      if (StartNameServer.debugMode) {
        GNS.getLogger().fine(" String = " + sb.toString());
      }
      if (sb.length() == size) {
        return sb.toString();
      } else {
        if (StartNameServer.debugMode) {
          GNS.getLogger().severe(" Size mismatch in reading paxos state. Msg size = " + size + " Actual size = " + sb.length());
        }
      }
    } catch (Exception e) {
      if (StartNameServer.debugMode) {
        GNS.getLogger().severe("Exception in reading paxos state from file. File:" + f.getAbsolutePath());
      }
      e.printStackTrace();
    }

    return null;
  }

  /**
   * returns the file name of the paxos instance
   * @param paxosID
   * @param packet
   * @return
   */
  private static String getStateLogFileName(String paxosID, StatePacket packet) {
    return getLogFolderPath() + "/" + stateFolderSubdir + "/" + paxosID + "_" + packet.b + "_" + packet.slotNumber;
  }
  private static String paxosIDFileComplete = null;

  /**
   *
   * @return
   */
  private static String getPaxosIDsFile() {
    if (paxosIDFileComplete == null) {
      paxosIDFileComplete = getLogFolderPath() + "/" + paxosIDsFile;
    }
    return paxosIDFileComplete;
  }

  private static String getPaxosStateFolder() {
    return paxosStateFolder;
  }

  /**
   * Get list of log files in sorted order.
   *
   * File names are of form <logFilePrefix><integer>, file name with higher integer values are
   * more recent logs.
   *
   * @return
   */
  private static String[] getSortedLogFileList() {

    String[] fileList = new File(getLogFolderPath()).list(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        if (name.startsWith(logFilePrefix)) {
          try {
            int logFile1 = getLogFileNumber(name);
          } catch (NumberFormatException e) {
            // if file number is not of form  "<logFilePrefix><integer>, then don't accept
            return false;
          }
          return true;
        }
        return false;
      }
    });

    Arrays.sort(fileList, new Comparator<String>() {
      @Override
      public int compare(String o1, String o2) {
        int logFile1 = getLogFileNumber(o1);
        int logFile2 = getLogFileNumber(o2);
        return logFile1 - logFile2;
      }
    });

    return fileList;
  }

  /**
   * TODO add doc here
   *
   * @param paxosInstances
   * @param paxosID
   * @param msg
   */
  public static void parsePaxosStart(ConcurrentHashMap<String, PaxosReplica> paxosInstances,
                                     String paxosID, String msg) {

    if (paxosInstances.containsKey(paxosID)) {
      if (StartNameServer.debugMode) {
        GNS.getLogger().severe(paxosID + "\tERROR: Paxos Instance already exists.");
      }
      return;
    }

    Set<Integer> nodeIDs = stringToSetInteger(msg);

    if (StartNameServer.debugMode) {
      GNS.getLogger().fine(paxosID + "\tPaxos Instance Added. NodeIDs: " + nodeIDs);
    }

    paxosInstances.put(PaxosName.getPaxosNameFromPaxosID(paxosID), new PaxosReplica(paxosID, PaxosManager.nodeID, nodeIDs));
  }

  /**
   * TODO add doc here
   * @param paxosInstances
   * @param paxosID
   */
  private static void parsePaxosStop(ConcurrentHashMap<String, PaxosReplica> paxosInstances, String paxosID,
                                     String msg) {
    PaxosReplica paxosReplica = paxosInstances.remove(paxosID);

    //TODO check this when implementing log synchronization

//    if (paxosReplica !=null) {
//      // add a paxos replica whose state is deleted.
//      try {
//        paxosInstances.remove()
//        paxosInstances.put(paxosID, new PaxosReplica(paxosID,PaxosManager.nodeID,paxosReplica.getNodeIDs(),true,
//                new RequestPacket(new JSONObject(msg))));
//      } catch (JSONException e) {
//        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//      }
//      if (StartNameServer.debugMode) GNS.getLogger().fine(paxosID + "\tPaxos Instance Removed");
//    }
//    else {
//      if (StartNameServer.debugMode) GNS.getLogger().fine(paxosID + "\tPaxos Instance NOT Removed");
//    }
  }

  /**
   * Add to the set of decisions in new replica
   *
   * @param paxosReplica
   * @param message
   */
  private static void parseDecision(PaxosReplica paxosReplica, String message) throws JSONException {
    if (paxosReplica != null && message != null) {
      ProposalPacket proposalPacket = new ProposalPacket(new JSONObject(message));
      paxosReplica.recoverDecision(proposalPacket);
    }
  }

  /**
   * TODO add doc here
   *
   * @param replica
   * @param msg
   */
  private static void parsePrepare(PaxosReplica replica, String msg) throws JSONException {
    if (replica != null && msg != null) {
      PreparePacket packet = new PreparePacket(new JSONObject(msg));
      replica.recoverPrepare(packet);
    }
  }

  /**
   * TODO add doc here
   *
   * @param replica
   * @param msg
   */
  private static void parseAccept(PaxosReplica replica, String msg) throws JSONException {
    if (replica != null && msg != null) {
      AcceptPacket packet = new AcceptPacket(new JSONObject(msg));
      replica.recoverAccept(packet);
    }
  }

  /**
   * extract log file number by parsing the the file name of log
   *
   * @param logFileName
   * @return log file number
   */
  private static int getLogFileNumber(String logFileName) throws NumberFormatException {
    String[] tokens = logFileName.split("/");
    String fileNumberStr = tokens[tokens.length - 1].substring(logFilePrefix.length());

    return Integer.parseInt(fileNumberStr);
  }

  /**
   * Add a msg to logging queue
   * @param command
   */
  static void logMessage(LoggingCommand command) {
    synchronized (logQueueLock) {
      logCommands.add(command);
    }
    if (StartNameServer.debugMode) {
      GNS.getLogger().fine(" Added msg to queue: " + command.getLogJson());
    }
  }

  private static String getNextFileName() {
    logFileNumber++;
    return getLogFolderPath() + "/" + logFilePrefix + logFileNumber;
  }

  /**
   * log the current msgs in queue
   */
  static void doLogging() {
    while (true) {
      ArrayList<LoggingCommand> logCmdCopy = null;

      synchronized (logQueueLock) {
        if (logCommands.size() > 0) {
          logCmdCopy = logCommands;
          logCommands = new ArrayList<LoggingCommand>();
        }

      }

      if (SLEEP_INTERVAL_MS > 0) {
        try {
          Thread.sleep(SLEEP_INTERVAL_MS);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

      if (logCmdCopy == null) {
        continue;
      }
//      continue;


      //        if (StartNameServer.debugMode) GNS.getLogger().fine(" Logging messages: " + msgsLogged.size());
      // log the msgs
//            StringBuilder sb = new StringBuilder();
//      char[] buf = new char[1000];
//      int index = 0;

      try {
        long t0 = System.currentTimeMillis();
        FileWriter fileWriter = new FileWriter(logFileName, true);
        for (LoggingCommand cmd : logCmdCopy) {
          // TODO How is BufferedWriter different from FileWriter? what should we use?
          String s = getLogString(cmd.getPaxosID(), cmd.getLogJson());
//          if (StartNameServer.debugMode) GNS.getLogger().fine("Logging this now: " + s);
          fileWriter.write(s);
        }
        fileWriter.close();
        long t1 = System.currentTimeMillis();
        if (t1 - t0 > 20) {
          GNS.getLogger().fine("Long latency Paxos logging = " + (t1 - t0) + " ms. Time = " + (t0) + " Msgcount = " + logCmdCopy.size());
        }
        msgCount += logCmdCopy.size();
        if (msgCount > MSG_MAX) {
          msgCount = 0;
          logFileName = getNextFileName();
        }
      } catch (IOException e) {
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      } catch (JSONException e) {
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }

      // process each msg
      for (LoggingCommand cmd : logCmdCopy) {
        if (cmd.getActionAfterLog() == LoggingCommand.LOG_AND_EXECUTE) {
          try {
            PaxosManager.executorService.submit(new HandlePaxosMessageTask(cmd.getLogJson(), cmd.getLogJson().getInt(PaxosPacketType.ptype)));
          } catch (JSONException e) {
            e.printStackTrace();
          }
        } else if (cmd.getActionAfterLog() == LoggingCommand.LOG_AND_SEND_MSG) {

          PaxosManager.sendMessage(cmd.getDest(), cmd.getSendJson());
        }

      }

//      for (JSONObject jsonObject : msgsLogged) {
//        try {
//          PaxosManager.executorService.submit(new HandlePaxosMessageTask(jsonObject, jsonObject.getInt(PaxosPacketType.ptype)));
//        } catch (JSONException e) {
//          e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//        }
//      }

    }
//
//      for (JSONObject jsonObject: msgsLogged) {
//
//
////                StringBuilder sb = new StringBuilder();
////                for (Iterator i = jsonObject.keys(); i.hasNext();) {
////                    try {
////                        sb.append(jsonObject.get((String) i.next())+ " ");
////                    } catch (JSONException e) {
////                        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
////                    }
////
////                }
////                sb.append("\n");
//
//        try {
//          String s = jsonObject.toString();
//          for (int i = 0; i < 20; i++) {
//            buf[index] = s.charAt(i);
//            index++;
//          }
//          buf[index]  = "\n".charAt(0);
//          index++;
//
//          if (index > 900) {
//            buf[index]  = "\n".charAt(0);
//            index++;
//            fileWriter.write(buf, 0, index);
//            index = 0;
//          }
////                    sb.append(jsonObject.toString().substring(0,20) + "\n");
////                    if (sb.length() > 900) {
////                        fileWriter.write(sb.toString());
////                        sb = new StringBuilder();
////                    }
//        } catch (IOException e) {
//          e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//        }
//
//
//      }
//
//      try {
//        if (index > 0) {
//          buf[index]  = "\n".charAt(0);
//          index++;
//          fileWriter.write(buf, 0, index);
//        }
//        fileWriter.flush();
//      } catch (IOException e) {
//        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//      }
//

  }

  private static String getLogString(String paxosID, int msgType, String msg) {
    return paxosID + "\t" + msgType + "\t" + msg + "\n";
  }

  private static String getLogString(String paxosID, JSONObject jsonObject) throws JSONException {
    StringBuilder sb = new StringBuilder();
    sb.append(paxosID);
    sb.append("\t");

    sb.append(jsonObject.get(PaxosPacketType.ptype));
    sb.append("\t");
    sb.append(jsonObject);
    sb.append("\n");
    return sb.toString();
  }

  /**
   * Delete state files for paxos instances that either (1) belong to paxos
   * instances that have been deleted (2) a newer state file for the same paxos instance
   * is logged.
   */
  static void deleteRedundantStateLogs() {
    String stateFolder = getPaxosStateFolder();
    File f = new File(stateFolder);
    String[] state = f.list(); // read names of all files

    HashMap<String, ArrayList<PaxosStateFileName>> allStateFiles = new HashMap<String, ArrayList<PaxosStateFileName>>();
    // stores all state file names for all paxos instances

    for (String filename : state) {
      try {
        PaxosStateFileName paxosName = new PaxosStateFileName(filename);
        if (!allStateFiles.containsKey(paxosName.paxosID)) {
          allStateFiles.put(paxosName.paxosID, new ArrayList<PaxosStateFileName>());

        }
        allStateFiles.get(paxosName.paxosID).add(paxosName);
//        else if (paxosLatestFile.get(paxosName.paxosID).compareTo(paxosName) < 0) {
//          // delete any not up to date
//        }
      } catch (Exception e) {
        GNS.getLogger().severe(" Incorrectly formatted paxosStateFile: " + filename + " Deleting file");
        File fDel = new File(stateFolder + "/" + filename);
        boolean result = fDel.delete();
        GNS.getLogger().severe(" State file delete output: " + result + " filename: " + filename);

      }
    }

    for (String paxosID : allStateFiles.keySet()) {
      ArrayList<PaxosStateFileName> files = allStateFiles.get(paxosID);

      // is paxos instance running?
      if (PaxosManager.paxosInstances.containsKey(paxosID)) {
        if (files.size() == 1) {
          continue;
        }
        // sort the files in increasing order
        Collections.sort(files);
        for (int i = files.size() - 1; i >= 0; i--) {
          File f1 = new File(paxosStateFolder + "/" + files.get(i).filename);
          String s = getPaxosStateFromFile(f1); // try reading state from disk
          if (s != null) { // if state successfully read, then delete previous log files
            if (StartNameServer.debugMode) {
              GNS.getLogger().fine("Most recent state file for paxos ID = " + paxosID + " is " + files.get(i).filename);
            }
            // delete all files whose index is less than 'i'
            for (int j = i - 1; j >= 0; j--) {
              File f2 = new File(paxosStateFolder + "/" + files.get(j).filename);
              boolean result = f2.delete();
              if (StartNameServer.debugMode) {
                GNS.getLogger().fine("Deleting older state file for paxos ID = " + paxosID + " File name = " + files.get(j).filename + " Result = " + result);
              }
            }
            break;
          }
        }
      } else { // delete all state files if paxos instance is stopped.
        for (int i = files.size() - 1; i >= 0; i--) {
          File f1 = new File(paxosStateFolder + "/" + files.get(i).filename);
          boolean result = f1.delete();
          if (StartNameServer.debugMode) {
            GNS.getLogger().fine("Deleting state file as paxos ID is stopped. paxos ID = " + paxosID + " File name = " + files.get(i).filename + " Result = " + result);
          }
        }
      }
    }

  }

  /**
   * Deletes any log message files that are no longer needed.
   *
   * Let 'X' be the set of paxos instances which have a message logged in a log file.
   * A log file can be deleted if for all x \in X, either (1) 'x' is stopped or (2) the latest state
   * for 'x' was logged on disk after all log messages for 'x' in this log file were written.
   */
  public static void deleteLogMessageFiles() {

    // select all log files
    String[] logFiles = getSortedLogFileList();
    for (int i = 0; i < logFiles.length - 1; i++) {
      String x = logFiles[i];
      if (isLogFileDeletable(x)) {
        File f = new File(getLogFolderPath() + "/" + x);
        boolean result = f.delete();
        if (StartNameServer.debugMode) {
          GNS.getLogger().fine("Deletable : " + x);
        }
//          if (StartNameServer.debugMode) GNS.getLogger().fine("NOT Deletable : " + x);
      } else {
        if (StartNameServer.debugMode) {
          GNS.getLogger().fine("NOT Deletable: " + x);
        }
      }

    }

  }

  public static void main(String[] args) throws IOException {

    for (int i = 0; i < 100000; i++) {
      long t0 = System.currentTimeMillis();
      FileWriter fileWriter = new FileWriter("/state/partition1/myfilename", true);
//      TODO How is BufferedWriter different from FileWriter? what should we use?
      String s = "klllkjasdl;fjaoial;smaso;imwa;eoimaw;cmaiw;coiamw;lcj;lhvalijwoij;lcmasdfasdfcawe;ojakls;dcl;w";
//      if (StartNameServer.debugMode) GNS.getLogger().fine("Logging this now: " + s);
      fileWriter.write(s);
      fileWriter.close();
      long t1 = System.currentTimeMillis();
      if (t1 - t0 > 20) {
        System.out.println("Long latency logging = " + (t1 - t0) + " ms. Time = " + (t0));
      }
    }

  }

  /**
   * returns true if log file can be deleted
   *
   * Let 'X' be the set of paxos instances which have a message logged in this log file.
   * Log file can be deleted if for all x \in X, either (1) 'x' is stopped or (2) the latest state
   * for 'x' was logged on disk after all log messages for 'x' in this log file were written.
   * @param logFileName
   * @return
   */
  static boolean isLogFileDeletable(String logFileName) {

    HashSet<String> paxosIDs = getPaxosInstanceSet(logFileName);

    filterStoppedPaxosIDs(paxosIDs);

    HashMap<String, PaxosStateFileName> paxosState = readPaxosState(paxosIDs);

    try {
      BufferedReader br = new BufferedReader(new FileReader(new File(getLogFolderPath() + "/" + logFileName)));
      int i = 0;
      while (true) {
        String line = br.readLine();
        if (line == null) {
          break;
        }
//        if (StartNameServer.debugMode) GNS.getLogger().fine("checking line: " + line);
        try {
          PaxosLogMessage logMessage = new PaxosLogMessage(line);
          if (paxosState.containsKey(logMessage.getPaxosID())) {
            if (isStateAfterLogMessage(logMessage, paxosState.get(logMessage.getPaxosID())) == false) {
              return false;
            }
          }
        } catch (Exception e1) {
          e1.printStackTrace();
          GNS.getLogger().severe(" Exception in parsing log message: " + line);

        }
      }
      br.close();
    } catch (IOException e) {
      e.printStackTrace();
    }


    return true;
  }

  /**
   * return set of paxos IDs which have a message in this log file
   * @param logFileName
   * @return Set of paxosIDs
   */
  private static HashSet<String> getPaxosInstanceSet(String logFileName) {
    HashSet<String> paxosIDs = new HashSet<String>();
    try {
      BufferedReader br = new BufferedReader(new FileReader(new File(getLogFolderPath() + "/" + logFileName)));
      while (true) {
        String line = br.readLine();
        if (line == null) {
          break;
        }
//        if (StartNameServer.debugMode) GNS.getLogger().fine("Reading line: " + line);
        PaxosLogMessage logMessage = new PaxosLogMessage(line);
        if (paxosIDs.contains(logMessage.getPaxosID()) == false) {
          paxosIDs.add(logMessage.getPaxosID());
        }
      }
      br.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return paxosIDs;
  }

  /**
   * remove paxosIDs from this set which have been stopped.
   * @param paxosIDs
   */
  private static void filterStoppedPaxosIDs(HashSet<String> paxosIDs) {
    HashSet<String> deleteIDs = new HashSet<String>();
    for (String x : paxosIDs) {
      if (PaxosManager.paxosInstances.containsKey(x)) {
        continue;
      }
      deleteIDs.add(x);
    }
    for (String x : deleteIDs) {
      paxosIDs.remove(x);
    }
  }

  /**
   * read the most up to date state for each paxos ID in {@code paxosIDs} from disk.
   * @param paxosIDs
   * @return
   */
  private static HashMap<String, PaxosStateFileName> readPaxosState(HashSet<String> paxosIDs) {

    String stateFolder = getPaxosStateFolder();
    File f = new File(stateFolder);
    String[] stateFiles = f.list(); // read names of all files

    // stores all state file names for all paxos instances
    HashMap<String, ArrayList<PaxosStateFileName>> paxosAllFiles = new HashMap<String, ArrayList<PaxosStateFileName>>();

    HashMap<String, PaxosStateFileName> paxosLatestFile = new HashMap<String, PaxosStateFileName>();

    for (String filename : stateFiles) {
      try {
        PaxosStateFileName paxosName = new PaxosStateFileName(filename);
        if (paxosIDs.contains(paxosName.paxosID) == false) {
          continue;
        }
        if (paxosAllFiles.containsKey(paxosName.paxosID) == false) {
          paxosAllFiles.put(paxosName.paxosID, new ArrayList<PaxosStateFileName>());
        }
        paxosAllFiles.get(paxosName.paxosID).add(paxosName);
      } catch (Exception e) {
        GNS.getLogger().severe(" Incorrectly formatted paxosStateFile: " + filename + " Deleting file");
        File fDel = new File(stateFolder + "/" + filename);
        boolean result = fDel.delete();
        GNS.getLogger().severe(" State file delete output: " + result + " filename: " + filename);

      }
    }

    for (String paxosID : paxosAllFiles.keySet()) {
      ArrayList<PaxosStateFileName> files = paxosAllFiles.get(paxosID);

      // sort the files in increasing order
      Collections.sort(files);
      for (int i = files.size() - 1; i >= 0; i--) {
        File f1 = new File(paxosStateFolder + "/" + files.get(i).filename);
        String s = getPaxosStateFromFile(f1); // try reading state from disk
        if (s != null) { // if state successfully read, then delete previous log files
          files.get(i).state = s;
          paxosLatestFile.put(paxosID, files.get(i));
          break;
        }
      }

    }


    return paxosLatestFile;  //To change body of created methods use File | Settings | File Templates.
  }

  /**
   * Returns true if the state was logged after this log message was written.
   * @param logMsg
   * @param stateFileName
   * @return
   */
  static boolean isStateAfterLogMessage(PaxosLogMessage logMsg, PaxosStateFileName stateFileName) {
    try {
      switch (logMsg.getLogMessageType()) {
        case PaxosPacketType.ACCEPT:
          AcceptPacket accept = new AcceptPacket(new JSONObject(logMsg.getMessage()));
//          boolean result = false;
//          if (stateFileName.slotNumber > accept.pValue.proposal.slot) result = true; // > sign is important
//          GNS.getLogger().fine("MSG1. Slot1 " + stateFileName.slotNumber  + " Slot2 = " + accept.pValue.proposal.slot + " result = " + result );
          if (stateFileName.slotNumber > accept.pValue.proposal.slot) {
            return true; // > sign is important
          } else {
            return false;
          }
        case PaxosPacketType.PREPARE:
          PreparePacket prepare = new PreparePacket(new JSONObject(logMsg.getMessage()));
//          result = false;
//          if (stateFileName.ballot.compareTo(prepare.ballot) >= 0) result = true; // notice >= here
//          GNS.getLogger().fine("MSG2. Ballot1 " + stateFileName.ballot + " Ballot2 = " + prepare.ballot + " result = " + result);
          if (stateFileName.ballot.compareTo(prepare.ballot) >= 0) {
            return true; // notice >= here
          } else {
            return false;
          }
        case PaxosPacketType.DECISION:
          ProposalPacket proposal = new ProposalPacket(new JSONObject(logMsg.getMessage()));
//          result = false;
//          if (stateFileName.slotNumber > proposal.slot) result = true; // > sign is important
//          GNS.getLogger().fine("MSG1. Slot1 " + stateFileName.slotNumber  + " Slot2 = " + proposal.slot + " result = " + result);
          if (stateFileName.slotNumber > proposal.slot) {
            return true; // > sign is important
          } else {
            return false;
          }
      }
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return true;
  }
}

/**
 * This thread does the logging for PREPARE, ACCEPT, and DECISON messages for all paxos instances.
 */
class LoggingThread extends Thread {

  @Override
  public void run() {
    try {

      PaxosLogger.doLogging();
    } catch (Exception e) {
      System.out.println(e.getMessage());
      e.printStackTrace();
    }
  }
}

/**
 * Periodically checks and deletes redundant paxos logs
 */
class LogDeletionTask extends TimerTask {

  @Override
  public void run() {
    PaxosLogger.deleteRedundantStateLogs();
    PaxosLogger.deleteLogMessageFiles();
  }
}

/**
 * Class parses the file name of a paxos state file and extracts paxosID, ballotNumber,
 * and slotNumber from the file
 */
class PaxosStateFileName implements Comparable {

  /**
   * paxosID of this paxos instance
   */
  String paxosID;
  /**
   * Ballot number of the paxos instance when this state was logged
   */
  Ballot ballot;
  /**
   * slotNumber of the paxos instance when this state was logged
   */
  int slotNumber;
  /**
   * complete file name
   */
  String filename;
  /**
   * State of paxos instance that is stored in this paxos file.
   */
  String state;

  public PaxosStateFileName(String name) throws Exception {
    this.filename = name;
    String[] tokens = name.split("_");
    if (tokens.length != 3) {
      return;
    }
    paxosID = tokens[0];
    ballot = new Ballot(tokens[1]);
    slotNumber = Integer.parseInt(tokens[2]);
  }

  public void updateState(String state) {
    this.state = state;
  }

  @Override
  public int compareTo(Object o) {
    PaxosStateFileName p = (PaxosStateFileName) o;
    int x = ballot.compareTo(p.ballot);
    if (x != 0) {
      return x;
    }
    if (slotNumber == p.slotNumber) {
      return 0;
    }
    return slotNumber - p.slotNumber;
  }
}

/**
 *
 */
class LoggingCommand {

  public static int LOG_AND_EXECUTE = 1;
  public static int LOG_AND_SEND_MSG = 2;
  private String paxosID;
  private JSONObject logJson;
  private int actionAfterLog;
  private int dest = -1;
  private JSONObject sendJson;

  public LoggingCommand(String paxosID, JSONObject logJson, int actionAfterLog) {
    this.paxosID = paxosID;
    this.actionAfterLog = actionAfterLog;
    this.logJson = logJson;
  }

  public LoggingCommand(String paxosID, JSONObject logJson, int actionAfterLog, int dest, JSONObject sendJson) {
    this.paxosID = paxosID;
    this.actionAfterLog = actionAfterLog;
    this.logJson = logJson;
    try {
      sendJson.put(PaxosManager.PAXOS_ID, paxosID);
    } catch (JSONException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
    this.sendJson = sendJson;
    this.dest = dest;
  }

  public JSONObject getLogJson() {
    return logJson;
  }

  public int getActionAfterLog() {
    return actionAfterLog;
  }

  public int getDest() {
    return dest;
  }

  public JSONObject getSendJson() {
    return sendJson;
  }

  public String getPaxosID() {
    return paxosID;
  }
}

/**
 * Class for a paxos log message that is read/written.
 */
class PaxosLogMessage {

  private String paxosID;
  private int logMessageType;
  private String message;

  public PaxosLogMessage(String paxosID, int logMessageType, String message) {
    this.paxosID = paxosID;
    this.logMessageType = logMessageType; //LogMessageType.getPacketType(logMessageType);
    this.message = message;
  }

  public PaxosLogMessage(String s) {
    String[] tokens = s.split("\\s");
    paxosID = tokens[0];
    logMessageType = Integer.parseInt(tokens[1]);
    message = s.substring(tokens[0].length() + tokens[1].length() + 2);
  }

  public String getPaxosID() {
    return paxosID;
  }

  public int getLogMessageType() {
    return logMessageType;
  }

  public String getMessage() {
    return message;
  }
}
