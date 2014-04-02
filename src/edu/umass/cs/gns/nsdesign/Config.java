package edu.umass.cs.gns.nsdesign;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.replicationframework.ReplicationFrameworkType;

import java.util.HashMap;

/**
 * Configuration parameters for name servers. This class contains static members which can be accessed from anywhere
 * in code.
 * Created by abhigyan on 3/27/14.
 */
public class Config {

  private static final String DEFAULTPAXOSLOGPATHNAME = "paxosLog";


  private static boolean initialized = false;

  public static boolean debugMode = true;

  public static boolean experimentMode = false;

  public static boolean singleNS = false;

  public static int numReplicaControllers = 3;



  // paxos parameters
  public static int failureDetectionTimeoutInterval = 30000;
  public static int failureDetectionPingInterval = 10000;
  public static String paxosLogFolder = DEFAULTPAXOSLOGPATHNAME;




  // active replica group change related parameters
  public static double normalizingConstant = 0.5;
  public static int minReplica = 3;
  public static int maxReplica = 100;
  public static ReplicationFrameworkType replicationFramework;
  public static int analysisIntervalMillis = 10000000;
  public static int movingAverageWindowSize = 20;
  public static int nameServerVoteSize = 5;
  public static final int NS_TIMEOUT_MILLIS = 2000;


  // testing related parameters
  public static boolean emulatePingLatencies = false;


  public static synchronized void initialize(HashMap<String, String> allValues) {
    if (initialized) return;

    initialized = true;



    if (allValues.containsKey(NSParameterNames.PRIMARY_REPLICAS)) {
      Config.numReplicaControllers = Integer.parseInt(allValues.get(NSParameterNames.PRIMARY_REPLICAS));
      GNS.numPrimaryReplicas = numReplicaControllers;
     }

    if (allValues.containsKey(NSParameterNames.FILE_LOGGING_LEVEL)) {
      GNS.fileLoggingLevel = allValues.get(NSParameterNames.FILE_LOGGING_LEVEL);
    }
    if (allValues.containsKey(NSParameterNames.CONSOLE_OUTPUT_LEVEL)) {
      GNS.consoleOutputLevel = allValues.get(NSParameterNames.CONSOLE_OUTPUT_LEVEL);
    }
    if (allValues.containsKey(NSParameterNames.STAT_FILE_LOGGING_LEVEL)) {
      GNS.statFileLoggingLevel = allValues.get(NSParameterNames.STAT_FILE_LOGGING_LEVEL);
    }
    if (allValues.containsKey(NSParameterNames.STAT_CONSOLE_OUTPUT_LEVEL)) {
      GNS.statConsoleOutputLevel = allValues.get(NSParameterNames.STAT_CONSOLE_OUTPUT_LEVEL);
    }

    if (allValues.containsKey(NSParameterNames.PAXOS_LOG_FOLDER)) {
      paxosLogFolder = allValues.get(NSParameterNames.PAXOS_LOG_FOLDER);
    } else {
      paxosLogFolder = DEFAULTPAXOSLOGPATHNAME;
    }

    if (allValues.containsKey(NSParameterNames.SINGLE_NS)) {
      numReplicaControllers = 1;
      singleNS = true;
    }

  }

}
