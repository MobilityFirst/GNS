package edu.umass.cs.gns.nsdesign;

import edu.umass.cs.gns.database.DataStoreType;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.replicationframework.ReplicationFrameworkType;

import java.util.HashMap;

/**
 * Should only contain config that is common across all name servers. Any name server specific configuration,
 * e.g.., nodeID, should be in gnsReconfigurable package or replicaController package.
 * Parameters are initialized once when name server is started, and thereafter accessed statically.
 *
 * Created by abhigyan on 3/27/14.
 */
public class Config {

  /** First version number for a GUID. */
  public static final int FIRST_VERSION = 1;

  public static final String NO_COORDINATOR_STATE_MARKER = "NoCoordinatorState";

  private static final String DEFAULTPAXOSLOGPATHNAME = "paxosLog";


  private static boolean initialized = false;

  public static boolean debugMode = false;

  public static boolean experimentMode = false;


//  public static int numReplicaControllers = 3;

  // paxos parameters
  public static int failureDetectionTimeoutSec = 30000;
  public static int failureDetectionPingSec = 10000;
  public static String paxosLogFolder = DEFAULTPAXOSLOGPATHNAME;

  public static DataStoreType dataStore = DataStoreType.MONGO;
  public static int mongoPort = 27017;

  // active replica group change related parameters
  public static double normalizingConstant = 0.5;
  public static int minReplica = 3;
  public static int maxReplica = 100;
  public static ReplicationFrameworkType replicationFrameworkType;
  public static int analysisIntervalSec = 100000;
  public static int movingAverageWindowSize = 20;
  public static int nameServerVoteSize = 5;
  public static final int NS_TIMEOUT_MILLIS = 5000;

  // testing related parameters
  public static boolean useGNSNIOTransport = true;
  public static boolean emulatePingLatencies = false;
  public static double latencyVariation = 0.1;
  public static boolean noPaxosLog = false;
  public static boolean singleNS = false;
  public static boolean readCoordination = false;


  public static synchronized void initialize(HashMap<String, String> allValues) {
    if (initialized) return;

    initialized = true;

    if (allValues.containsKey(NSParameterNames.PRIMARY_REPLICAS)) {
      GNS.numPrimaryReplicas = Integer.parseInt(allValues.get(NSParameterNames.PRIMARY_REPLICAS));
//      GNS.numPrimaryReplicas = numReplicaControllers;
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

    if (allValues.containsKey(NSParameterNames.FAILURE_DETECTION_MSG_INTERVAL)) {
      failureDetectionPingSec = Integer.parseInt(allValues.get(NSParameterNames.FAILURE_DETECTION_MSG_INTERVAL));
    }

    if (allValues.containsKey(NSParameterNames.FAILURE_DETECTION_TIMEOUT_INTERVAL)) {
      failureDetectionTimeoutSec = Integer.parseInt(allValues.get(NSParameterNames.FAILURE_DETECTION_TIMEOUT_INTERVAL));
    }

    if (allValues.containsKey(NSParameterNames.SINGLE_NS)) {
      GNS.numPrimaryReplicas = 1;
      singleNS = true;
    }

    if (allValues.containsKey(NSParameterNames.SINGLE_NS)) {
      GNS.numPrimaryReplicas = 1;
      singleNS = true;
    }

    if (allValues.containsKey(NSParameterNames.READ_COORDINATION)) {
      readCoordination = Boolean.parseBoolean(allValues.get(NSParameterNames.READ_COORDINATION));
    }
    if (allValues.containsKey(NSParameterNames.EMULATE_PING_LATENCIES)) {
      emulatePingLatencies = Boolean.parseBoolean(allValues.get(NSParameterNames.EMULATE_PING_LATENCIES));
      if (allValues.containsKey(NSParameterNames.VARIATION)) {
        latencyVariation = Double.parseDouble(allValues.get(NSParameterNames.VARIATION));
      }
      GNS.getLogger().info("Emulating ping latency at name server: emulatePingLatencies = " + emulatePingLatencies);
    }

    if (allValues.containsKey(NSParameterNames.NO_PAXOS_LOG)) {
      noPaxosLog = Boolean.parseBoolean(allValues.get(NSParameterNames.NO_PAXOS_LOG));
    }

    if (allValues.containsKey(NSParameterNames.MONGO_PORT)) {
      mongoPort = Integer.parseInt(allValues.get(NSParameterNames.MONGO_PORT));
    }

    if (allValues.containsKey(NSParameterNames.DEBUG_MODE)) {
      debugMode = Boolean.parseBoolean(allValues.get(NSParameterNames.DEBUG_MODE));
    }

    if (allValues.containsKey(NSParameterNames.STATIC)) {
      replicationFrameworkType = ReplicationFrameworkType.STATIC;
    } else if (allValues.containsKey(NSParameterNames.LOCATION)) {
      replicationFrameworkType = ReplicationFrameworkType.LOCATION;
    } else if (allValues.containsKey(NSParameterNames.RANDOM)) {
      replicationFrameworkType = ReplicationFrameworkType.RANDOM;
    }

    if (allValues.containsKey(NSParameterNames.REPLICATION_INTERVAL)) {
      analysisIntervalSec = Integer.parseInt(allValues.get(NSParameterNames.REPLICATION_INTERVAL));
    }
    if (allValues.containsKey(NSParameterNames.NORMALIZING_CONSTANT)) {
      normalizingConstant = Double.parseDouble(allValues.get(NSParameterNames.NORMALIZING_CONSTANT));
    }
    if (allValues.containsKey(NSParameterNames.USE_GNS_NIO_TRANSPORT)) {
      useGNSNIOTransport = Boolean.parseBoolean(allValues.get(NSParameterNames.USE_GNS_NIO_TRANSPORT));
    }

  }

}
