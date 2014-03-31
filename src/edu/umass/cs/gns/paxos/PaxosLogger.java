package edu.umass.cs.gns.paxos;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.paxos.paxospacket.*;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * NOTE: This class is extremely important for GNS. After restarting a name server, we depend on this class to recover
 * the data stored at the node.
 *
 * Logging class used to log paxos messages and state for all paxos instances. It also recovers the database
 * state on restarting the system.
 *
 * All logs are stored in folder {@code paxosLogFolder}. Log folder contains 3 types of files:
 *
 * (1) Paxos IDs file: This file is stored in {@code paxosIDsFile}. It logs two types of events: (1) a paxos instance is created
 * (2)a paxos instance is stopped. This is an infinite log.
 * On startup, this {@code paxosIDsFile} tells which paxos instances were active before the system stopped.
 *
 * (2) Paxos state: These files are stored in folder {@code logFolder}/{@code stateFolderSubdir}. It contains periodic
 * snapshots of complete state of all active paxos instance. This complete snapshot includes values of variables
 * internal to paxos protocol as well as the state in database corresponding to this paxos system.
 * The name of the state file contains values of variables internal to paxos and the contents of the file
 * are the database state corresponding to this paxos instance. The name of each file is formatted as follows:
 * 'paxosID_ballot_slotNumber'. The ballot is formatted as 'ballotNumber:coordinatorID'. The content of each file is
 * as follows. The first line contains an integer represented in string format. The integer tells the
 * the length of the paxos state contained in the file. Lines after the first line contain the state of the paxos instance.
 *
 * There could be multiple state files for same paxos instance. In this case, the our naming convention
 * helps know which is the most recent state file for a paxos instance: a file with higher ballot number of slot number
 * is more recent.
 *
 * (3) Log message files: This files are stored '{@code logFolder}/{@code logFilePrefix}_X' where 'X' is a non-negative
 * integer. These files the paxos messages as as they are processed. Each file contains at most {@code MSG_MAX} messages.
 * After {@code MSG_MAX} are logged to file {@code logFilePrefix}_X, further message are logged to
 * {@code logFilePrefix}_(X+1). A single log message file contains messages belonging to several paxos instances.
 *
 *
 * This class does three sets of actions: logging, log recovery, and log deletion.
 *
 * (1) Logging: Messages to be logged in are queued. A logging thread checks the queue periodically,
 * logs messages, and then performs the action associated with every logging message.
 * Currently, we log three types of messages: PREPARE, ACCEPT, and DECISION.  For example, once the DECISION message
 * is logged, it is forwarded to the paxos instance for processing.
 *
 * We use queuing of messages for two reasons:(1) It  the logging thread to log multiple messages with a single
 * disk seek operation (2) For the paxos replica object, logging is an operation that does not block for IO activity.
 * The queuing adds some complexity to the design because the logging thread need to take some action with each
 * message after completing the logging. Refer to documentation on <code>LoggingCommand</code> on what these actions
 * are.
 *
 * Some logging tasks are not done via the queue. These include messages logged when paxos instances is added/removed.
 * and the periodic logging of complete snapshot of paxos replicas. These are infrequent operations, therefore, even if
 * we were to log these messages via queue, the performance benefits will be small.
 *
 *
 * (2) Log recovery: The log recovery recreates the set of paxos instances that were active at this node, and for
 * each instance it recovers the internal state of the paxos object, as well as the contents of the database state.
 * The recovery process consists of three steps: (a) we read the set of paxos instances at this node from the paxos IDs
 * file (b) read the most recent complete snapshot of each paxos instances (c) parse the log message files, and update
 * paxos instances based on messags that were logged after the complete snapshot of paxos instances.
 *
 *
 * (3) Log deletion: Log deletion involves two tasks (a) if there are multiple files storing snapshots for the same
 * paxos instance, we keep the most recent snapshot and delete other files (b) delete redundant log message files.
 * Some other log message files could be safe to delete because they might contain state for paxos instances that
 * no longer exist, or because we have taken a snapshot of all paxos instances after the messages in this file
 * were logged.
 *
 *
 * Limitations of design:
 * (1) {@code logFolder}/{@code stateFolderSubdir} contains state for all paxos instances in separate files in the
 * same directory. This may cause problems if the file system performs poorly while handling a large number of files
 * in the same directory.
 * Future work: Use nesting of directories to store snapshot of paxos state of different instances. This will
 * require changes to log recovery, and log deletion as well.
 *
 *
 * User: abhigyan
 * Date: 7/24/13
 * Time: 8:16 AM
 */
public class PaxosLogger extends Thread {


  /**
   * Folder where paxos logs are stored
   */
  private String logFolder = null;

  /**
   * Node ID of this node
   */
  private int nodeID = -1;

  /**
   * This is the paxos manager object for which logger is doing the logging.
   */
  private PaxosManager paxosManager;

  /**
   * Lock object controlling access to {@code logCommands}
   */
  private  final Object logQueueLock = new ReentrantLock();

  /**
   * Messages currently queued for logging
   */
  private  ArrayList<LoggingCommand> logCommands = new ArrayList<LoggingCommand>();

  /**
   * If {@code msgs} is empty (no new messages for logging), then the logging thread
   * will sleep for {@code SLEEP_INTERVAL_MS} before checking the {@code msgs} again.
   */
  private static int SLEEP_INTERVAL_MS = 1;

  /**
   * Names of paxos log files start with following prefix.
   */
  private  String logFilePrefix = "paxoslog_";

  /**
   * Name of file where logs are currently being stored
   */
  private String logFileName;

  /**
   * File number of log file currently used for logging. This file number is included in
   * parameter <code>logFileName</code>.
   */
  private int logFileNumber = 0;

  /**
   * Number of messages written to current log file. Once <code>msgCount</code> becomes more than
   * <code>MSG_MAX</code>, we start writing to the next log file.
   */
  private int msgCount = 0;

  /**
   * After writing {@code MSG_MAX} messages to a file, a new log file is used.
   */
  private int MSG_MAX = 10000;

  /**
   * Name of file which stores the list of paxos instances at a node.
   * This is a single file which will contain all instances created and deleted since the
   * system started running. This file could contain records of paxos instances that have
   * been deleted long ago. The size of this file grows very slowly,
   * but it could grow unbounded.
   *
   * Future work: periodically, we can create new versions of this file after removing records of paxos instances
   * that have been deleted so that this file is of finite size.
   */
  private static final String paxosIDsFile = "paxosIDs";

  /**
   * Lock object used to isolate access to writes to paxos IDs file.
   */
  private  final ReentrantLock paxosIDsLock = new ReentrantLock();

  /**
   * This sub-directory periodically stores a the complete state of all paxos instances. Periodic logging
   * of the complete paxos state allows us to garbage collect older logs, as the state file already contains
   * information given in previous logs.
   */
  private  String stateFolderSubdir = "paxosState";

  /**
   * This is the complete path name of <code>stateFolderSubdir</code>.
   */
  private  String paxosStateFolder = null;

  /**
   * This variable is always set to true except during post-processing of paxos logs
   * that are collected during a test run of GNS. See class {@code edu.umass.cs.gns.paxos.PaxosLogAnalyzer} for the
   * code that does the analysis.
   */
  private  boolean gnsRunning = true;

  private boolean debugMode = false;

  /************************START  OF CONSTRUCTORS********************************/

  /**
   * Create a new paxos logger object which will store logs in the given folder.
   * This method initializes the variables in the class and creates folders for paxos logs (if they do not exist).
   * If paxos logs are already existing, it does not recover data from logs from modify the logs in any way.
   * @param logFolder Folder where logs will be stored.
   * @param nodeID Node ID of this node.
   * @param paxosManager Paxos Manager object for this this logger is doing the logging.
   */
  public PaxosLogger(String logFolder, int nodeID, PaxosManager paxosManager) {
    this.logFolder = logFolder;
    this.nodeID = nodeID;
    this.paxosManager = paxosManager;
    paxosStateFolder = getLogFolderPath() + "/" + stateFolderSubdir;

    createLogDirs();
    logFileName = getNextFileName();

  }


  /**
   * This constructor is to be used only by class {@code edu.umass.cs.gns.paxos.PaxosLogAnalyzer},
   * which does an post-processing of paxos logs collected during a test run of GNS. Do not use this for anything else.
   *
   * @param logFolder Folder where logs are stored.
   * @param nodeID  Node ID of this node.
   * @param gnsRunning Set to true to perform offline analysis of logs.
   */
  public PaxosLogger(String logFolder, int nodeID, boolean gnsRunning) {
    this.logFolder = logFolder;
    this.nodeID = nodeID;
    this.gnsRunning = gnsRunning;
    paxosStateFolder = getLogFolderPath() + "/" + stateFolderSubdir;
  }

  /************************END OF CONSTRUCTORS********************************/


  /************************START OF PUBLIC METHODS IN PAXOS LOGGER********************************/

  /**
   * Starts the logging thread.
   */
  @Override
  public void run() {
    try {
      doLogging();
    } catch (Exception e) {
      System.out.println(e.getMessage());
      e.printStackTrace();
    }
  }

  /**
   * This method is called on startup. It recovers state from existing logs when paxos starts up.
   * @return {@code ConcurrentHashMap} of {@code PaxosReplica} objects recovered from logs, {@code null} if no logs
   * exist. Keys of {@code ConcurrentHashMap} are paxos keys, not paxos IDs. See method
   * <code>getPaxosKey</code>).
   */
  ConcurrentHashMap<String, PaxosReplicaInterface> recoverPaxosLogs() {

    // initialize folder
    ConcurrentHashMap<String, PaxosReplicaInterface> replicas;
    replicas = recoverPaxosInstancesFromLogs();

    logFileName = getNextFileName();
    GNS.getLogger().info(" Logger Initialized.");

    if (debugMode) {
      GNS.getLogger().fine(" File Writer created.");
    }

    if (debugMode) {
      GNS.getLogger().fine(" Thread started.");
    }

    return replicas;
  }


  /**
   * Add a msg to logging queue. Use this method to log all messages except messages that
   * logged when a paxos instance is created and when it is stopped. Use methods <code>logPaxosStart</code>
   * and  <code>logPaxosStop</code> in those cases.
   *
   * @param command <code>LoggingCommand</code> which includes paxosID of the paxos instance, the message and
   *                the action the paxos logger should take after logging the message.
   */
  void logMessage(LoggingCommand command) {
    synchronized (logQueueLock) {
      logCommands.add(command);
    }
    if (debugMode) {
      GNS.getLogger().fine(" Added msg to queue: " + command.getLogJson());
    }
  }

  /**
   * Logs to disk that this paxos instance is created. We log information in two files:
   * (1) We log the paxosID and the set of nodes in <code>paxosIDsFile</code>, which contains a list of
   * paxos instances at this node.
   * (2) We log the initialState in a separate file in the folder <code>paxosStateFolder</code>.
   *
   * @param paxosID
   * @param nodeIDs
   * @param initialState
   */
  void logPaxosStart(String paxosID, Set<Integer> nodeIDs, StatePacket initialState) {
    if (debugMode) {
      GNS.getLogger().fine(" Paxos ID = " + paxosID);
    }
    if (debugMode) {
      GNS.getLogger().fine(" Node IDs = " + nodeIDs);
    }
    if (debugMode) {
      GNS.getLogger().fine(" Initial state = " + initialState.state);
    }
    String paxosIDsFile1 = getPaxosIDsFile();
    if (paxosIDsFile1 != null) {
      synchronized (paxosIDsLock) {
        // first log initial state
        logPaxosState(paxosID, initialState);
        // then append to paxos IDs
        String logString = getLogString(paxosID, PaxosPacketType.START, setIntegerToString(nodeIDs));
        appendToFile(paxosIDsFile1, logString);
      }
    }
  }

  /**
   * Log to disk that this paxosID is stopped. This message is logged in <code>paxosIDsFile</code>.
   *
   * This method must be called after a paxos instance is removed from this node, which happens usually after
   * a paxos replica has executed the STOP command. An uncommon case is when
   * a paxos replica has not executed the STOP command, but this node receives information to create
   * a new paxos instance with the same key as the existing paxos replica. In GNS, this can happen during
   * group change if a node is member of both old and new paxos replicas. See method <code>createPaxosInstance</code>
   * in <code>PaxosManager</code> for more clarification.
   *
   * @param paxosID <code>paxosID</code> of the paxos instance.
   */
  void logPaxosStop(String paxosID) {

    String paxosIDsFile1 = getPaxosIDsFile();

    synchronized (paxosIDsLock) {
      String logString = null;
      logString = getLogString(paxosID, PaxosPacketType.STOP, Integer.toString(PaxosPacketType.STOP));

      appendToFile(paxosIDsFile1, logString);
    }
  }

  /**
   * Log the complete state of this paxosID which is included in the <code>StatePacket</code>.
   * This state will be stored in folder <code>paxosStateFolder</code>.
   * @param paxosID <code>paxosID</code> of the paxos instance.
   * @param packet <code>StatePacket</code> containing information about paxos state.
   */
  void logPaxosState(String paxosID, StatePacket packet) {

    synchronized (paxosIDsLock) {
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
        e.printStackTrace();  
      }
    }
  }

  /**
   * Delete state files for paxos instances that either (1) belong to paxos
   * instances that have been deleted (2) a newer state file for the same paxos instance
   * is logged.
   */
  void deleteRedundantStateLogs() {
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
      if (paxosManager.paxosInstances.containsKey(getPaxosKey(paxosID))) {
        if (files.size() == 1) {
          continue;
        }
        // sort the files in increasing order
        Collections.sort(files);
        for (int i = files.size() - 1; i >= 0; i--) {
          File f1 = new File(paxosStateFolder + "/" + files.get(i).filename);
          String s = getPaxosStateFromFile(f1); // try reading state from disk
          if (s != null) { // if state successfully read, then delete previous log files
            if (debugMode) {
              GNS.getLogger().fine("Most recent state file for paxos ID = " + paxosID + " is " + files.get(i).filename);
            }
            // delete all files whose index is less than 'i'
            for (int j = i - 1; j >= 0; j--) {
              File f2 = new File(paxosStateFolder + "/" + files.get(j).filename);
              boolean result = f2.delete();
              if (debugMode) {
                GNS.getLogger().fine("Deleting older state file for paxos ID = " + paxosID + " File name = " +
                        files.get(j).filename + " Result = " + result);
              }
            }
            break;
          }
        }
      } else { // delete all state files if paxos instance is stopped.
        for (int i = files.size() - 1; i >= 0; i--) {
          File f1 = new File(paxosStateFolder + "/" + files.get(i).filename);
          boolean result = f1.delete();
          if (debugMode) {
            GNS.getLogger().fine("Deleting state file as paxos ID is stopped. paxos ID = " + paxosID + " File name = "
                    + files.get(i).filename + " Result = " + result);
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
  void deleteLogMessageFiles() {

    // select all log files
    String[] logFiles = getSortedLogFileList();
    for (int i = 0; i < logFiles.length - 1; i++) {
      String x = logFiles[i];
      if (isLogFileDeletable(x)) {
        File f = new File(getLogFolderPath() + "/" + x);
        boolean result = f.delete();
        if (debugMode) {
          GNS.getLogger().fine("Deletable : " + x);
        }
//          if (debugMode) GNS.getLogger().fine("NOT Deletable : " + x);
      } else {
        if (debugMode) {
          GNS.getLogger().fine("NOT Deletable: " + x);
        }
      }

    }

  }

  /**
   * Clear all the paxos logs. Used for testing only.
   */
  void clearLogs() {
    if (logFolder != null) {
      getLogFolderPath().length();
      File f = new File(getLogFolderPath());
      deleteDir(f);
      createLogDirs();// recreate log dirs if they do not exist.
    }

  }

  /**
   * Method to be used only by {@code PaxosLogAnalyzer} class.
   *
   * This method reads all paxos logs and stores the log messages in {@code PaxosReplica} object.
   * @return {@code ConcurrentHashMap} of {@code PaxosReplica} objects that includes log messages, {@code null}
   * if no object exists. Keys of {@code ConcurrentHashMap} are paxosIDs, values are {@code PaxosReplica} objects.
   */
  ConcurrentHashMap<String, PaxosReplicaInterface> readAllPaxosLogs() {
//    this.nodeID = nodeID;
    // step 1: recover list of paxos instances
    ConcurrentHashMap<String, PaxosReplicaInterface> paxosInstances = recoverListOfPaxosInstances();
    if (paxosInstances == null || paxosInstances.size() == 0) {
      return paxosInstances;
    }

    // no need to read paxos log state
    // step 3: read paxos logs for messages received after the paxos state was logged
    recoverLogMessagesAfterLoggedState(paxosInstances);


    if (debugMode) {
      GNS.getLogger().fine("Paxos Recovery: Complete.");
    }

    return paxosInstances;
  }


  /************************END OF PUBLIC METHODS IN PAXOS LOGGER********************************/





  /************************Start of private methods called during logging process********************************/

  /**
   * log the current msgs in queue
   */
  private void doLogging() {
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

      try {
        long t0 = System.currentTimeMillis();
        FileWriter fileWriter = new FileWriter(logFileName, true);
        for (LoggingCommand cmd : logCmdCopy) {
          // TODO How is BufferedWriter different from FileWriter? what should we use?
          fileWriter.write(cmd.getPaxosID());
          fileWriter.write("\t");
          fileWriter.write(Integer.toString(cmd.getLogJson().getInt(PaxosPacketType.ptype)));
          fileWriter.write("\t");
          fileWriter.write(cmd.getLogJson().toString());
          fileWriter.write("\n");
        }
        fileWriter.close();
        long t1 = System.currentTimeMillis();
        if (t1 - t0 > 50) {
          GNS.getLogger().warning("Long latency Paxos logging = " + (t1 - t0) + " ms. Time = " + (t0) + " MsgCount = "
                  + logCmdCopy.size());
        }
        msgCount += logCmdCopy.size();
        if (msgCount > MSG_MAX) {
          msgCount = 0;
          logFileName = getNextFileName();
        }
      } catch (IOException e) {
        e.printStackTrace();  
      } catch (JSONException e) {
        e.printStackTrace();  
      }
      // ** Logging end **

      // process each msg
      for (LoggingCommand cmd : logCmdCopy) {
        if (cmd.getActionAfterLog() == LoggingCommand.LOG_AND_EXECUTE) {
          try {
            paxosManager.executorService.submit(new HandlePaxosMessageTask(cmd.getLogJson(),
                    cmd.getLogJson().getInt(PaxosPacketType.ptype), paxosManager));
          } catch (JSONException e) {
            e.printStackTrace();
          }
        } else if (cmd.getActionAfterLog() == LoggingCommand.LOG_AND_SEND_MSG) {
          paxosManager.sendMessage(cmd.getDest(), cmd.getSendJson(), cmd.getPaxosID());
        }

      }

    }

  }


  /**
   * Creates folders where paxos logs are stored.
   *
   * A separate folder is created for storing most recent state of paxos instances, that is periodically logged.
   */
  private  void createLogDirs() {
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
   * The name of the file in which we will log the paxos state. The name is generated based on fields in the
   * <code>StatePacket</code>.
   *
   * @param paxosID <code>paxosID</code> of the paxos instance.
   * @param packet <<code>StatePacket</code> containing information about paxos state.
   * @return complete path name of the file where paxos state will be logged.
   */
  private  String getStateLogFileName(String paxosID, StatePacket packet) {
    return getLogFolderPath() + "/" + stateFolderSubdir + "/" + paxosID + "_" + packet.b + "_" + packet.slotNumber;
  }


  /**
   * Returns a string in the format it can be logged to file.
   * The format of the message is: PaxosID<TAB>MsgType<TAB>Msg
   * @param paxosID The log msg belong to this paxosID
   * @param msgType Type of message
   * @param msg Log message
   * @return
   */
  private  String getLogString(String paxosID, int msgType, String msg) {
    return paxosID + "\t" + msgType + "\t" + msg + "\n";
  }


  /**
   * This method performs the same role as <code>getLogString</code> above, except,
   * the log message is given as a json object. The method extract msg type from logs.
   * @param paxosID
   * @param jsonObject
   * @return
   * @throws JSONException
   */
  private  String getLogString(String paxosID, JSONObject jsonObject) throws JSONException {
    StringBuilder sb = new StringBuilder();
    sb.append(paxosID);
    sb.append("\t");

    sb.append(jsonObject.get(PaxosPacketType.ptype));
    sb.append("\t");
    sb.append(jsonObject);
    sb.append("\n");
    return sb.toString();
  }

  /************************End of private methods for doing logging********************************/


  /************************Start of private methods for log recovery********************************/

  /**
   * This method recovers paxos state from logs at system startup
   * @return {@code ConcurrentHashMap} of {@code PaxosReplica} objects recovered from logs, {@code null}
   * if no object exists. Keys of {@code ConcurrentHashMap} are paxos keys, not paxosIDs. See method
   * getPaxosKey).
   */
  private  ConcurrentHashMap<String, PaxosReplicaInterface> recoverPaxosInstancesFromLogs() {

    // step 1: recover list of paxos instances
    ConcurrentHashMap<String, PaxosReplicaInterface> paxosInstances = recoverListOfPaxosInstances();
    if (paxosInstances == null) {
      return null;
    }

    // step 2: recover most recent state logged for each paxos instance
    recoverMostRecentStateOfPaxosInstances(paxosInstances);

    // step 3: read paxos logs for messages received after the paxos state was logged
    GNS.getLogger().fine("key set: " + paxosInstances.keySet());

    recoverLogMessagesAfterLoggedState(paxosInstances);

    if (debugMode) {
      GNS.getLogger().fine("Paxos Recovery: Complete.");
    }

    return paxosInstances;
  }

  /**
   * Parses the file  <code>paxosIDsFile</code> and obtains a list of paxos instances currently
   * active at this node, and the list of nodes that are member  of paxos instance.
   * @return A <code>ConcurrentHashMap</code> whose key is the paxosKey obtained from <code>paxosID</code>,
   * and value is a <code>PaxosReplicaInterface</code> object.
   */
  private  ConcurrentHashMap<String, PaxosReplicaInterface> recoverListOfPaxosInstances() {

    ConcurrentHashMap<String, PaxosReplicaInterface> paxosInstances = new ConcurrentHashMap<String, PaxosReplicaInterface>();

    File f = new File(getPaxosIDsFile());

    if (!f.exists()) {
      if (debugMode) {
        GNS.getLogger().fine("Paxos Recovery: " + getPaxosIDsFile() + " does not exist. "
                + "No further recovery possible.");
      }
      return new ConcurrentHashMap<String, PaxosReplicaInterface>();
    }

    if (debugMode) {
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

    if (debugMode) {
      GNS.getLogger().fine("Paxos Recovery: completed reading paxos IDs file. "
              + "Number of Paxos IDs = " + paxosInstances.size());
    }
    if(gnsRunning) appendToFile(getPaxosIDsFile(), "\n"); // appends newline to log so that incomplete messages from the crash of the
                                           // system terminates in newline
    return paxosInstances;
  }

  /**
   * This is the second phase of log recovery after the list of paxos instances has been read.
   * For each paxos instance in the list of instances, we find the file with the most recent snapshot of state for
   * this paxos instance. We update the paxos instance based on the state found in this file.
   * @param paxosInstances <code>ConcurrentHashMap</code> with list of paxos instances.
   */
  private  void recoverMostRecentStateOfPaxosInstances(ConcurrentHashMap<String, PaxosReplicaInterface> paxosInstances) {
    GNS.getLogger().fine("Keys in paxos instance: " + paxosInstances.keySet());
    File f = new File(getPaxosStateFolder());
    if (f.exists() == false) {
      if (debugMode) {
        GNS.getLogger().severe("ERROR: the folder which stores most recent state of paxos instances does not exist.");
      }
      return;
    }
    File[] files = f.listFiles();

    ConcurrentHashMap<String, PaxosStateFileName> mostRecentStateFile = new ConcurrentHashMap<String,
            PaxosStateFileName>();
    for (File f1 : files) {
      try {
        PaxosStateFileName fName = new PaxosStateFileName(f1.getName());
        if (!paxosInstances.containsKey(getPaxosKey(fName.paxosID)) ||
                !fName.paxosID.equals(paxosInstances.get(getPaxosKey(fName.paxosID)).getPaxosID())) {
          continue;
        }
        String state = getPaxosStateFromFile(f1);
        if (state == null) {
          continue;
        } else {
          fName.updateState(state);
        }

        if (mostRecentStateFile.containsKey(fName.paxosID) == false) {
          GNS.getLogger().fine(" Put state in file " + f1.getName() + " for paxos ID " + fName.paxosID);
          mostRecentStateFile.put(fName.paxosID, fName);
        } else if (fName.compareTo(mostRecentStateFile.get(fName.paxosID)) > 0) {
          GNS.getLogger().fine(" REPLACE: state in file " + f1.getName() + " for paxos ID " + fName.paxosID);
          mostRecentStateFile.put(fName.paxosID, fName);
        } else {
          GNS.getLogger().fine(" IGNORE: state in file " + f1.getName() + " for paxos ID " + fName.paxosID);
        }
      } catch (Exception e) {
        GNS.getLogger().fine(" ERROR Parsing log state file name: " + f1.getName());
      }
    }

    for (String paxosKey: paxosInstances.keySet()) {
      String paxosID = paxosInstances.get(paxosKey).getPaxosID();
      if (mostRecentStateFile.containsKey(paxosID) == false) {
        GNS.getLogger().severe("ERROR: No state logged for paxos instance = " + paxosKey +
                "\tThis case should not happen.");
      } else {
        GNS.getLogger().fine("Recovering state for : " + paxosID);
        PaxosStateFileName state = mostRecentStateFile.get(paxosID);
        paxosInstances.get(paxosKey).recoverCurrentBallotNumber(state.ballot);
        paxosInstances.get(paxosKey).recoverSlotNumber(state.slotNumber);
        paxosManager.clientRequestHandler.updateState(paxosID, state.state);
      }
    }
  }

  /**
   * This is last phase of log recovery which recovers state stored in the log message files.
   * For each paxos instance, we discard any message that were written before the most recent snapshot for
   * that instance was taken, as the second phase has already completed recovery until the most recent snapshot.
   * We update paxos instances with messages that were written after the most recent snapshot.
   * @param paxosInstances <<code>ConcurrentHashMap</code> with list of paxos instances.
   */
  private  void recoverLogMessagesAfterLoggedState(ConcurrentHashMap<String, PaxosReplicaInterface> paxosInstances) {

    // get list of log files
    String[] fileList = getSortedLogFileList();

    if (debugMode) {
      GNS.getLogger().fine("Paxos Recovery: number of log files found: " + fileList.length);
    }

    if (fileList.length > 0) {
      String fileName = fileList[fileList.length - 1];
      // logFileNumber is the highest log file number found in log folder
      logFileNumber = getLogFileNumber(fileName);
      // we will choose the current logFileNumber one more the highest logFileNumber in the log folder.
    }

    for (int i = 0; i < fileList.length; i++) {
      if (debugMode) {
        GNS.getLogger().fine("Paxos Recovery: Now recovering log file: " + fileList[i]);
      }
      try {
        BufferedReader br = new BufferedReader(new FileReader(new File(getLogFolderPath() + "/" + fileList[i])));
        while (true) {
          String line = br.readLine();
          if (line == null) {
            break;
          }
          if (debugMode) {
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
    if (gnsRunning) {
      for (String paxosKey : paxosInstances.keySet()) {
        paxosInstances.get(paxosKey).executeRecoveredDecisions();
      }
    }
  }

  /**
   * Parse a single line from paxos logs and update the set {@code paxosInstances} accordingly.
   * This method parses lines found in both <code>paxosIDsFile</code> and the log message files.
   *
   * @param paxosInstances set of paxos instances
   * @param line paxos log line from either <code>paxosIDsFile</code> or the log message files.
   */
  private  void parseLine(ConcurrentHashMap<String, PaxosReplicaInterface> paxosInstances, String line) {
    if (line.length() <= 1) {
      return;
    }
    PaxosLogMessage logMessage;
    try {
      // construct a log message object
      logMessage = new PaxosLogMessage(line);
      // handle update to paxos instances

      updatePaxosInstances(paxosInstances, logMessage);
      if (debugMode) {
        GNS.getLogger().fine("Recovered log msg: " + line);
      }

    } catch (Exception e) {
      e.printStackTrace();
      if (debugMode) {
        GNS.getLogger().fine("Exception in recovering log msg: " + line);
      }
      return;
    }

  }

  /**
   * This method updates the <code>ConcurrentHashMap</code> containing list of paxos instances,
   * based on a single log message.
   * @param paxosInstances <<code>ConcurrentHashMap</code> with list of paxos instances.
   * @param logMessage log message to be processed
   * @throws JSONException
   */
  private  void updatePaxosInstances(ConcurrentHashMap<String, PaxosReplicaInterface> paxosInstances,
                                           PaxosLogMessage logMessage) throws JSONException {
    if (paxosInstances == null || logMessage == null) {
      if (debugMode) {
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
        parseDecision(paxosInstances.get(getPaxosKey(logMessage.getPaxosID())),
                logMessage.getMessage());
        break;
      case PaxosPacketType.ACCEPT:
        parseAccept(paxosInstances.get(getPaxosKey(logMessage.getPaxosID())),
                logMessage.getMessage());
        break;
      case PaxosPacketType.PREPARE:
        parsePrepare(paxosInstances.get(getPaxosKey(logMessage.getPaxosID())),
                logMessage.getMessage());
        break;
      // Abhigyan: I am not deleting these because we may be adding some of these log messages
//      case BALLOT:
//        parseCurrentBallot(paxosInstances.get(logMessage.getPaxosID()), logMessage.getMessage());
//        break;
//      case SLOTNUMBER:
//        parseCurrentSlotNumber(paxosInstances.get(logMessage.getPaxosID()), logMessage.getMessage());
//        break;
//      case GARBAGESLOT:
//        parseGarbageCollectionSlot(paxosInstances.get(logMessage.getPaxosID()), logMessage.getMessage());
//        break;
//      case PaxosPacketType.PREPARE:
//        parsePValue(paxosInstances.get(logMessage.getPaxosID()), logMessage.getMessage());
//        break;
    }
  }

  /**
   * Read the file containing a snapshot of the paxos state of a paxos replica object .
   * The file contains the state stored in database for this paxos instance.
   * This method returns the database state in a string form.
   */
  private  String getPaxosStateFromFile(File f) {

    try {
      StringBuilder sb = new StringBuilder();
      BufferedReader br = new BufferedReader(new FileReader(f));
      String x = br.readLine();
      int size = Integer.parseInt(x);
      if (debugMode) {
        GNS.getLogger().fine("Filename: " + f.getAbsolutePath() + " Size = " + size);
      }
      int lc = 0; // line count
      Runtime.getRuntime().exec("cat " + f.getAbsolutePath());
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
      if (debugMode) {
        GNS.getLogger().fine("Filename: " + f.getAbsolutePath() + " String = " + sb.toString());
      }
      if (sb.length() == size) {
        return sb.toString();
      } else {
        if (debugMode) {
          GNS.getLogger().severe(" Size mismatch in reading paxos state. Msg size = " + size + " Actual size = " +
                  sb.length());
        }
      }
    } catch (Exception e) {
      if (debugMode) {
        GNS.getLogger().severe("Exception in reading paxos state from file. File:" + f.getAbsolutePath() +
                ". Printing contents of file.");
        try {
          Runtime.getRuntime().exec("cat " + f.getAbsolutePath());
        } catch (IOException e1) {
          e1.printStackTrace();
        }
      }
      e.printStackTrace();
    }

    return null;
  }


  /**
   * Get list of log files in sorted order.
   *
   * File names are of form <logFilePrefix><integer>, file name with higher integer values are
   * more recent logs.
   *
   * @return  String array where i-th element is the name of the i-th log file.
   */
  private  String[] getSortedLogFileList() {

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
   * Parse the line in <code>paxosIDsFile</code> logged upon the creation of a new paxos instance.
   * This method create a <code>PaxosReplicaInterface</code> object after parsing the line, and
   * adds it to the given <code>ConcurrentHashMap</code>.
   *
   * @param paxosInstances <code>ConcurrentHashMap</code> with current list of paxos instances.
   * @param paxosID <<code>paxosID</code> of this paxos instance.
   * @param msg
   */
  private   void parsePaxosStart(ConcurrentHashMap<String, PaxosReplicaInterface> paxosInstances,
                                     String paxosID, String msg) {

    if (paxosInstances.containsKey(getPaxosKey(paxosID)) &&
            paxosInstances.get(getPaxosKey(paxosID)).getPaxosID().equals(paxosID)) {
      if (debugMode) {
        GNS.getLogger().severe(paxosID + "\tERROR: Paxos Instance already exists.");
      }
      return;
    }

    Set<Integer> nodeIDs = stringToSetInteger(msg);

    if (debugMode) {
      GNS.getLogger().fine(paxosID + "\tPaxos Instance Added. NodeIDs: " + nodeIDs);
    }

    paxosInstances.put(getPaxosKey(paxosID), paxosManager.createPaxosReplicaObject(paxosID, paxosManager.nodeID, nodeIDs));
  }

  /**
   * Parse the line in <code>paxosIDsFile</code> logged after a paxos instance is removed by <code>PaxosManager</code>.
   * If a <code>PaxosReplicaInterface</code> objects with the same paxosID exists in the <code>ConcurrentHashMap</code>,
   * this method removes it
   * @param paxosInstances <code>ConcurrentHashMap</code> with current list of paxos instances.
   * @param paxosID <<code>paxosID</code> of the paxos instance.
   */
  private  void parsePaxosStop(ConcurrentHashMap<String, PaxosReplicaInterface> paxosInstances, String paxosID,
                                     String msg) {

    if (gnsRunning) { //
      GNS.getLogger().info("Checking paxos instances ... " + paxosID + "\t " + paxosInstances.keySet());
      if (paxosInstances.containsKey(getPaxosKey(paxosID)) &&
              paxosInstances.get(getPaxosKey(paxosID)).getPaxosID().equals(paxosID)) {
        GNS.getLogger().info("Removing paxos instances ... " + paxosID);
        paxosInstances.remove(getPaxosKey(paxosID));
      }
    }
    else { //
      PaxosReplicaInterface paxosReplica = paxosInstances.get(paxosID);
      if (paxosReplica != null) paxosReplica.recoverStop();
    }

  }

  /**
   * Parse a message logged after a paxos replica commits a given request. (We also use term decision for committed
   * requests). We pass the committed request to the <code>PaxosReplicaInterface</code> object.
   */
  private  void parseDecision(PaxosReplicaInterface paxosReplica, String message) throws JSONException {
    if (paxosReplica != null && message != null) {
      ProposalPacket proposalPacket = new ProposalPacket(new JSONObject(message));
      paxosReplica.recoverDecision(proposalPacket);
    }
  }

  /**
   *  Parse a prepare message logged by the paxos replica. We pass the prepare message to the
   *  <code>PaxosReplicaInterface</code> object.
   */
  private  void parsePrepare(PaxosReplicaInterface replica, String msg) throws JSONException {
    if (replica != null && msg != null) {
      PreparePacket packet = new PreparePacket(new JSONObject(msg));
      replica.recoverPrepare(packet);
    }
  }

  /**
   * Parse a accept message logged by the paxos replica. We pass the accept message to the
   *  <code>PaxosReplicaInterface</code> object.
   */
  private  void parseAccept(PaxosReplicaInterface replica, String msg) throws JSONException {
    if (replica != null && msg != null) {
      AcceptPacket packet = new AcceptPacket(new JSONObject(msg));
      replica.recoverAccept(packet);
    }
  }

  /**
   * extract log file number by parsing the the file name of log.
   *
   * @param logFileName
   * @return log file number
   */
  private  int getLogFileNumber(String logFileName) throws NumberFormatException {
    String[] tokens = logFileName.split("/");
    String fileNumberStr = tokens[tokens.length - 1].substring(logFilePrefix.length());

    return Integer.parseInt(fileNumberStr);
  }

  /************************End of private methods for log recovery********************************/


  /********************Start of private methods for log garbage collection*******************/

  /**
   * Returns true if log file can be deleted.
   *
   * Let 'X' be the set of paxos instances which have a message logged in this log file.
   * Log file can be deleted if for all x \in X, either (1) 'x' is stopped or (2) the latest state
   * for 'x' was logged on disk after all log messages for 'x' in this log file were written.
   * @param logFileName  file name
   * @return
   */
  private boolean isLogFileDeletable(String logFileName) {

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
   * Returns true if the state was logged after this log message was written.
   * @param logMsg
   * @param stateFileName
   * @return
   */
  private boolean isStateAfterLogMessage(PaxosLogMessage logMsg, PaxosStateFileName stateFileName) {
    try {
      switch (logMsg.getLogMessageType()) {
        case PaxosPacketType.ACCEPT:
          AcceptPacket accept = new AcceptPacket(new JSONObject(logMsg.getMessage()));
          if (stateFileName.slotNumber > accept.pValue.proposal.slot) {
            return true; // > sign is important
          } else {
            return false;
          }
        case PaxosPacketType.PREPARE:
          PreparePacket prepare = new PreparePacket(new JSONObject(logMsg.getMessage()));
          if (stateFileName.ballot.compareTo(prepare.ballot) >= 0) {
            return true; // notice >= here
          } else {
            return false;
          }
        case PaxosPacketType.DECISION:
          ProposalPacket proposal = new ProposalPacket(new JSONObject(logMsg.getMessage()));

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

  /**
   * return set of paxos IDs which have a message in this log file
   * @param logFileName
   * @return Set of paxosIDs
   */
  private  HashSet<String> getPaxosInstanceSet(String logFileName) {
    HashSet<String> paxosIDs = new HashSet<String>();
    try {
      BufferedReader br = new BufferedReader(new FileReader(new File(getLogFolderPath() + "/" + logFileName)));
      while (true) {
        String line = br.readLine();
        if (line == null) {
          break;
        }
//        if (debugMode) GNS.getLogger().fine("Reading line: " + line);
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
  private  void filterStoppedPaxosIDs(HashSet<String> paxosIDs) {
    HashSet<String> deleteIDs = new HashSet<String>();
    for (String x : paxosIDs) {
      if (paxosManager.paxosInstances.containsKey(getPaxosKey(x))) {
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
  private  HashMap<String, PaxosStateFileName> readPaxosState(HashSet<String> paxosIDs) {

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


  /********************End of private methods for log garbage collection*******************/


  /************************Start of other methods***********************/


  private  String getLogFolderPath() {
    // ADDED NODE + NameServer.nodeID so we can run multiple servers on a single machine
    // while specifying the same logFolder parameter
    return logFolder + "/" + "NODE" + nodeID;
  }

  private  String getPaxosKey(String paxosID) {
    if (gnsRunning) return paxosManager.getPaxosKeyFromPaxosID(paxosID);
    return paxosID;
  }

  /**
   *
   * @return
   */
  private  String getPaxosIDsFile() {

    return getLogFolderPath() + "/" + paxosIDsFile;
  }

  private  String getPaxosStateFolder() {
    return paxosStateFolder;
  }


  /**
   * TODO add doc here
   *
   * @param filename
   * @param logString
   */
  private  void appendToFile(String filename, String logString) {
    try {
      FileWriter fw = new FileWriter(filename, true);
      fw.write(logString);
      fw.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }


  private  String setIntegerToString(Set<Integer> integerSet) {
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

  private  Set<Integer> stringToSetInteger(String string) {
//    System.out.println(string);
    Set<Integer> integerSet = new HashSet<Integer>();
    String[] tokens = string.split(":");
    for (String s : tokens) {
      integerSet.add(Integer.parseInt(s));
    }
    return integerSet;
  }


  private  String getNextFileName() {
    logFileNumber++;
    return getLogFolderPath() + "/" + logFilePrefix + logFileNumber;
  }

  /**
   * Recursively deletes the given file
   * @param f
   */
  private  void deleteDir(File f) {
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

  /************************End of other methods***********************/


  public  void main(String[] args) throws IOException {

    for (int i = 0; i < 100000; i++) {
      long t0 = System.currentTimeMillis();
      FileWriter fileWriter = new FileWriter("/state/partition1/myfilename", true);
//      TODO How is BufferedWriter different from FileWriter? what should we use?
      String s = "klllkjasdl;fjaoial;smaso;imwa;eoimaw;cmaiw;coiamw;lcj;lhvalijwoij;lcmasdfasdfcawe;ojakls;dcl;w";
//      if (debugMode) GNS.getLogger().fine("Logging this now: " + s);
      fileWriter.write(s);
      fileWriter.close();
      long t1 = System.currentTimeMillis();
      if (t1 - t0 > 20) {
        System.out.println("Long latency logging = " + (t1 - t0) + " ms. Time = " + (t0));
      }
    }

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
  public static  int LOG_AND_SEND_MSG = 2;
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
      e.printStackTrace();  
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
    String[] tokens = s.split("\\s+");
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



/**
 * Periodically logs state of all paxos instances. Logging complete state allows garbage
 * collection of previous state.
 *
 * This method scans the list of paxos instances at each node, given in the
 * field <code>paxosInstances</code> in paxos manager, reads the state for that instance
 * by calling method <code>getState</code> in paxos replica object, and calls the logger
 * module to log state to disk.
 *
 * WARNING: We have never tested this code for even 10K or 100K paxos instances. So, we don't
 * know how system performance will be affected during logging for a large number of paxos  instances,
 * or how long it takes to log state.
 *
 */
class LogPaxosStateTask extends TimerTask {


  PaxosManager paxosManager;

  PaxosLogger paxosLogger;

  public LogPaxosStateTask(PaxosManager paxosManager, PaxosLogger paxosLogger) {
    this.paxosManager = paxosManager;
    this.paxosLogger = paxosLogger;
  }

  @Override
  public void run() {
    try {

//      if (StartNameServer.experimentMode) {return;} // we do not log paxos state during experiments ..

      GNS.getLogger().info("Logging paxos state task.");

      for (String paxosKey: paxosManager.paxosInstances.keySet()) {

        PaxosReplicaInterface replica = paxosManager.paxosInstances.get(paxosKey);
        if (paxosKey != null) {
          StatePacket packet = replica.getState();
          if (packet != null) {
            paxosLogger.logPaxosState(replica.getPaxosID(), packet);
          }
        }
      }
      GNS.getLogger().info("Completed logging.");
    }catch(Exception e) {
      // this exception is there because executor service does not print stack trace during exceptions.
      GNS.getLogger().severe("Exception IN paxos state logging " + e.getMessage());
      e.printStackTrace();
    }
  }

}


/**
 * Periodically checks and deletes redundant paxos logs
 */
class LogDeletionTask extends TimerTask {

  PaxosLogger paxosLogger;

  public LogDeletionTask(PaxosLogger paxosLogger) {
    this.paxosLogger = paxosLogger;
  }

  @Override
  public void run() {
    try {
      paxosLogger.deleteRedundantStateLogs();
      paxosLogger.deleteLogMessageFiles();
    } catch (Exception e) {
      GNS.getLogger().severe("Exception in Paxos log deletion. " + e.getMessage());
      e.printStackTrace();
    }
  }
}
