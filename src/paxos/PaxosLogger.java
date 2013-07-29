package paxos;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartNameServer;
import edu.umass.cs.gns.packet.paxospacket.PValuePacket;
import edu.umass.cs.gns.packet.paxospacket.ProposalPacket;
import edu.umass.cs.gns.packet.paxospacket.RequestPacket;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created with IntelliJ IDEA. User: abhigyan Date: 6/26/13 Time: 11:37 AM To change this template use File | Settings | File
 * Templates.
 */
public class PaxosLogger {

  private static final ReentrantLock loggingLock = new ReentrantLock();
  private static String paxosIDsFile = "paxosIDs";
  private static String logFilePrefix = "paxoslog_";
  private static int currentLogFileNumber = 1;
  private static int charCount = 0;
  private static int MAX_CHARS = 1024 * 1024 * 20;
  /**
   * File which stores all paxos logs
   */
  public static String logFolder;

  public static ConcurrentHashMap<String, PaxosReplica> initializePaxosLogger() {
    if (logFolder == null) {
      return null;
    }

    File f = new File(logFolder);
    if (!f.exists()) {
      f.mkdirs();
      return null;
    }

    return recoverPaxosInstancesFromLogs();
  }

  /**
   * Used for testing only.
   */
  public static void clearLogs() {
    if (logFolder != null) {
      logFolder.length();
      File f = new File(logFolder);
      if (!f.exists()) {
        return;
      }
      File[] f1 = f.listFiles();
      for (File f2 : f1) {
        if (f2.exists() && f2.isDirectory() == false) {
          f2.delete();
        }
      }
    }

  }

  public static ConcurrentHashMap<String, PaxosReplica> recoverPaxosInstancesFromLogs() {
    // step 1: read paxosIDs active currently
    ConcurrentHashMap<String, PaxosReplica> paxosInstances = new ConcurrentHashMap<String, PaxosReplica>();

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

    // get list of log files
    String[] fileList = getSortedLogFileList();

    if (StartNameServer.debugMode) {
      GNS.getLogger().fine("Paxos Recovery: number of log files found: " + fileList.length);
    }

    // choose the current log file number one more the highest logFileNumber in the log folder.
    if (fileList.length > 0) {
      String fileName = fileList[fileList.length - 1];
      int fileNumber = getLogFileNumber(fileName);
      currentLogFileNumber = fileNumber + 1;
    }
    if (StartNameServer.debugMode) {
      GNS.getLogger().fine("Paxos Recovery: future logs will be written in log file: "
              + logFilePrefix + currentLogFileNumber);
    }

    for (int i = fileList.length - 1; i >= 0; i--) {
      if (StartNameServer.debugMode) {
        GNS.getLogger().fine("Paxos Recovery: Now recovering log file: " + fileList[i]);
      }
      try {
        BufferedReader br = new BufferedReader(new FileReader(new File(logFolder + "/" + fileList[i])));
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
    }

    if (StartNameServer.debugMode) {
      GNS.getLogger().fine("Paxos Recovery: Complete.");
    }

    return paxosInstances;
  }

  private static void parseLine(ConcurrentHashMap<String, PaxosReplica> paxosInstances, String line) {
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
      case PAXOSSTART:
        parsePaxosStart(paxosInstances, logMessage.getPaxosID(), logMessage.getMessage());
        break;
      case PAXOSSTOP:
        parsePaxosStop(paxosInstances, logMessage.getPaxosID(), logMessage.getMessage());
        break;
      case DECISION:
        parseDecisionVariable(paxosInstances.get(logMessage.getPaxosID()), logMessage.getMessage());
        break;
      case PVALUE:
        parsePValue(paxosInstances.get(logMessage.getPaxosID()), logMessage.getMessage());
        break;
      case BALLOT:
        parseCurrentBallot(paxosInstances.get(logMessage.getPaxosID()), logMessage.getMessage());
        break;
      case SLOTNUMBER:
        parseCurrentSlotNumber(paxosInstances.get(logMessage.getPaxosID()), logMessage.getMessage());
        break;
      case GARBAGESLOT:
        parseGarbageCollectionSlot(paxosInstances.get(logMessage.getPaxosID()), logMessage.getMessage());
        break;
    }

  }

  /**
   * run garbage collection for Paxos logs
   */
  public static void runGarbageCollectionForPaxosLogs() {
    // TODO implement garbage collection
  }

  /**
   * get list of log files in sorted order file names are of form <logFilePrefix><integer>, file name with higher integer values are
   * more recent logs
   *
   * @return
   */
  public static String[] getSortedLogFileList() {

    String[] fileList = new File(logFolder).list(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        if (name.startsWith(logFilePrefix)) {
          try {
            int logFile1 = getLogFileNumber(name);
          } catch (NumberFormatException e) {
            // if file number is not of form  "<logFilePrefix><integer>, then dont accept
            return false;
          }
          System.out.println("xxxx" + name);
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
   * get log file number based on file name of log
   *
   * @param logFileName
   * @return
   */
  private static int getLogFileNumber(String logFileName) throws NumberFormatException {
    String[] tokens = logFileName.split("/");
    String fileNumberStr = tokens[tokens.length - 1].substring(logFilePrefix.length());

    return Integer.parseInt(fileNumberStr);
  }

    FileWriter fw = null;

  /**
   * file name currently being used for logging
   *
   * @return
   */
  private static String getCurrentLogFile() {
      int charCount = 1;
    if (logFolder != null) {
      if (PaxosLogger.charCount > MAX_CHARS) {
        currentLogFileNumber += 1;
        PaxosLogger.charCount = 0;
      }
      PaxosLogger.charCount += charCount;
      return logFolder + "/" + logFilePrefix + currentLogFileNumber;
    } else {
      return null;
    }
  }

  /**
   *
   * @return
   */
  private static String getPaxosIDsFile() {
    if (logFolder != null) {
      return logFolder + "/" + paxosIDsFile;
    } else {
      return null;
    }
  }

  /**
   * ****************************************************************
   * START: methods related to logging different types of messages and parsing the logged messages
   * *********************************************************************
   */
  /**
   * *
   * TODO add doc here
   *
   * @param paxosID
   * @param nodeIDs
   */
  public static void logPaxosStart(String paxosID, Set<Integer> nodeIDs) {
    String paxosIDsFile1 = getPaxosIDsFile();
    if (paxosIDsFile1 != null) {
      synchronized (loggingLock) {
        String logString = getLogString(paxosID, PaxosLogMessage.LogMessageType.PAXOSSTART, setIntegerToString(nodeIDs));
        appendToFile(paxosIDsFile1, logString);
      }
    }

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

    paxosInstances.put(paxosID, new PaxosReplica(paxosID, PaxosManager.nodeID, nodeIDs));
  }

    /**
     * TODO add doc here
     * @param paxosID
     */
    public static void logPaxosStop(String paxosID, RequestPacket requestPacket) {

        String paxosIDsFile1 = getPaxosIDsFile();

        synchronized (loggingLock) {
            String logString = null;
            try {
                logString = getLogString(paxosID, PaxosLogMessage.LogMessageType.PAXOSSTOP,
                        requestPacket.toJSONObject().toString());
            } catch (JSONException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                return;
            }
            appendToFile(paxosIDsFile1,logString);
        }
    }


    /**
     * TODO add doc here
     * @param paxosInstances
     * @param paxosID
     */
    private static void parsePaxosStop(ConcurrentHashMap<String, PaxosReplica> paxosInstances, String paxosID,
                                       String msg) {
        PaxosReplica paxosReplica = paxosInstances.remove(paxosID);
        if (paxosReplica !=null) {
            // add a paxos replica whose state is deleted.
            try {
                paxosInstances.put(paxosID, new PaxosReplica(paxosID,PaxosManager.nodeID,paxosReplica.getNodeIDs(),true,
                        new RequestPacket(new JSONObject(msg))));
            } catch (JSONException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
            if (StartNameServer.debugMode) GNS.getLogger().fine(paxosID + "\tPaxos Instance Removed");
        }
        else {
            if (StartNameServer.debugMode) GNS.getLogger().fine(paxosID + "\tPaxos Instance NOT Removed");
        }
    }

  /**
   * TODO add doc here
   *
   * @param paxosID
   * @param garbageCollectionSlot
   */
  public static void logGarbageCollectionSlot(String paxosID, int garbageCollectionSlot) {
    synchronized (loggingLock) {
      String logString = getLogString(paxosID, PaxosLogMessage.LogMessageType.GARBAGESLOT,
              Integer.toString(garbageCollectionSlot));
      String fileName = getCurrentLogFile();
      if (fileName != null) {
        appendToFile(fileName, logString);
      }
    }
  }

  /**
   *
   * @param replica
   * @param msg
   */
  private static void parseGarbageCollectionSlot(PaxosReplica replica, String msg) {
    if (replica == null || msg == null) {
      return;
    }
    int garbageCollectionSlot = Integer.parseInt(msg);
    replica.recoverGarbageCollectionSlot(garbageCollectionSlot);
  }

    public static void logDecisionAndSlotNumber(String paxosID, int slotNumber, ProposalPacket proposalPacket) {
        synchronized (loggingLock) {
            String fileName = getCurrentLogFile();
            try {
                FileWriter fw = new FileWriter(fileName, true);
                if (proposalPacket.req != null) {
                    fw.write(paxosID + "\t" + Integer.toString(PaxosLogMessage.LogMessageType.DECISION.getInt()) + "\t" + proposalPacket.toString()  + "\n");
                }

                fw.write(paxosID + "\t" + Integer.toString(PaxosLogMessage.LogMessageType.SLOTNUMBER.getInt()) + "\t" + Integer.toString(slotNumber));
                fw.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
  /**
   * TODO add doc here
   *
   * @param paxosID
   * @param slotNumber
   */
  public static void logCurrentSlotNumber(String paxosID, int slotNumber) {

    synchronized (loggingLock) {
        String fileName = getCurrentLogFile();
        try {
            FileWriter fw = new FileWriter(fileName, true);
            fw.write(paxosID + "\t" + Integer.toString(PaxosLogMessage.LogMessageType.SLOTNUMBER.getInt()) + "\t" + Integer.toString(slotNumber));
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
  }

  /**
   *
   * @param replica
   * @param msg
   */
  private static void parseCurrentSlotNumber(PaxosReplica replica, String msg) {
    if (replica == null || msg == null) {
      return;
    }
    int slotNumber = Integer.parseInt(msg);
    replica.recoverSlotNumber(slotNumber);

  }

  /**
   * TODO add doc here
   *
   * @param paxosID
   * @param b
   */
  public static void logCurrentBallot(String paxosID, Ballot b) {
    synchronized (loggingLock) {
      String logString = getLogString(paxosID, PaxosLogMessage.LogMessageType.BALLOT,
              b.toString());
      String fileName = getCurrentLogFile();
      if (fileName != null) {
        appendToFile(fileName, logString);
      }
    }
  }

  /**
   * TODO add doc here
   *
   * @param replica
   * @param msg
   */
  private static void parseCurrentBallot(PaxosReplica replica, String msg) {
    if (replica == null || msg == null) {
      return;
    }
    Ballot b = new Ballot(msg);
    replica.recoverCurrentBallotNumber(b);
  }

  /**
   * add decision to paxos logs
   *
   * @param paxosID
   * @param proposalPacket
   */
  public static void logDecision(String paxosID, ProposalPacket proposalPacket) {

    String msg;
    try {

      msg = proposalPacket.toJSONObject().toString();
    } catch (JSONException e) {
      e.printStackTrace();
      return;
    }
    synchronized (loggingLock) {
        String fileName = getCurrentLogFile();
        try {

            FileWriter fw = new FileWriter(fileName, true);
            fw.write(paxosID + "\t" + Integer.toString(PaxosLogMessage.LogMessageType.DECISION.getInt()) + "\t" + msg  + "\n");
//              paxosID + "\t" + msgType.getInt() + "\t" + msg + "\n";
//              fw.write(logString);
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

//        String logString = getLogString(paxosID, PaxosLogMessage.LogMessageType.DECISION, msg);

//      if (fileName != null) {
//        appendToFile(fileName, logString);
//      }
    }
  }

  /**
   * Add to the set of decisions in new replica
   *
   * @param paxosReplica
   * @param message
   */
  private static void parseDecisionVariable(PaxosReplica paxosReplica, String message) throws JSONException {
    if (paxosReplica == null || message == null) {
      return;
    }
    ProposalPacket proposalPacket = new ProposalPacket(new JSONObject(message));
    paxosReplica.recoverDecision(proposalPacket);
  }

  /**
   * log the pvalue
   *
   * @param paxosID
   * @param pValuePacket
   */
  public static void logPValue(String paxosID, PValuePacket pValuePacket) {
    String msg;
    try {
      msg = pValuePacket.toJSONObject().toString();
    } catch (JSONException e) {
      e.printStackTrace();
      return;
    }
    synchronized (loggingLock) {
//      String logString = getLogString(paxosID, PaxosLogMessage.LogMessageType.PVALUE, msg);
      String fileName = getCurrentLogFile();
      if (fileName != null) {
          try {

              FileWriter fw = new FileWriter(fileName, true);
              fw.write(paxosID + "\t" + Integer.toString(PaxosLogMessage.LogMessageType.PVALUE.getInt())+ "\t" + msg + "\n");
//              paxosID + "\t" + msgType.getInt() + "\t" + msg + "\n";
//              fw.write(logString);
              fw.close();
          } catch (IOException e) {
              e.printStackTrace();
          }
//        appendToFile(fileName, logString);
      }
    }
  }

  /**
   * TODO add doc here
   *
   * @param replica
   * @param msg
   */
  private static void parsePValue(PaxosReplica replica, String msg) throws JSONException {
    PValuePacket packet = new PValuePacket(new JSONObject(msg));
    replica.recoverPValue(packet);
  }

  /**
   * **********************************************************************
   * END: methods related to logging different types of messages parsing the logged messages
   * *************************&&&&****************************************
   */
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

  public static String getLogString(String paxosID, PaxosLogMessage.LogMessageType msgType, String msg) {
    return paxosID + "\t" + msgType.getInt() + "\t" + msg + "\n";
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
    System.out.println(string);
    Set<Integer> integerSet = new HashSet<Integer>();
    String[] tokens = string.split(":");
    for (String s : tokens) {
      integerSet.add(Integer.parseInt(s));
    }
    return integerSet;
  }
}

class PaxosLogMessage {

  public enum LogMessageType {

    BALLOT(1),
    DECISION(2),
    PVALUE(3),
    SLOTNUMBER(4),
    PAXOSSTART(5),
    PAXOSSTOP(6),
    GARBAGESLOT(7);
    private int number;

    private LogMessageType(int number) {
      this.number = number;
    }
    private static final Map<Integer, LogMessageType> map = new HashMap<Integer, LogMessageType>();

    static {
      for (LogMessageType type : LogMessageType.values()) {
        if (map.containsKey(type.getInt())) {
          GNS.getLogger().warning("**** Duplicate ID number for packet type " + type + ": " + type.getInt());
        }
        map.put(type.getInt(), type);
      }
    }

    public int getInt() {
      return number;
    }

    public static LogMessageType getPacketType(int number) {
      return map.get(number);
    }
  }
  private String paxosID;
  private LogMessageType logMessageType;
  private String message;

  public PaxosLogMessage(String paxosID, int logMessageType, String message) {
    this.paxosID = paxosID;
    this.logMessageType = LogMessageType.getPacketType(logMessageType);
    this.message = message;
  }

  public PaxosLogMessage(String s) {
    String[] tokens = s.split("\\s");
    paxosID = tokens[0];
    logMessageType = LogMessageType.getPacketType(Integer.parseInt(tokens[1]));
    message = s.substring(tokens[0].length() + tokens[1].length() + 2);
  }

  public String getPaxosID() {
    return paxosID;
  }

  public LogMessageType getLogMessageType() {
    return logMessageType;
  }

  public String getMessage() {
    return message;
  }
}

//class IncorrectLogMessageFormatException extends Exception{
//    private String logMessage;
//
//    public IncorrectLogMessageFormatException(String s) {
//        super(s);
//        logMessage = s;
//    }
//
//    public  String getLogMessage() {
//        return logMessage;
//    }
//}
class DeleteLogTask extends TimerTask {

  @Override
  public void run() {
    PaxosLogger.runGarbageCollectionForPaxosLogs();
  }
}
