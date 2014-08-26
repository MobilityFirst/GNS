package edu.umass.cs.gns.nsdesign;

import org.apache.commons.cli.*;

/**
 * List of configuration parameters for a name servers. Config file and command line arguments
 * use the string constants to set the corresponding parameters.
 * <p>
 * If a parameter is added, also update the list of command line options in method {{@link #getAllOptions()}}.
 * <p>
 * Future work: group parameter names into following: active replica options, replica controller options,
 * replica coordination options, experiment-related options.
 * <p>
 * Created by abhigyan on 2/28/14.
 */
public class NSParameterNames {

  public static final String CONFIG_FILE = "configFile";
  public static final String HELP = "help";
  public static final String ID = "id";
  public static final String NS_FILE = "nsfile";
  public static final String LOCAL_NS_COUNT = "localnscount";
  public static final String LOCAL_LNS_COUNT = "locallnscount";
  public static final String PRIMARY_REPLICAS = "primary";

  // replication-related parameters
  public static final String LOCATION = "location";
  public static final String STATIC = "static";
  public static final String RANDOM = "random";
  public static final String BEEHIVE = "beehive";

  public static final String AGGREGATE_INTERVAL = "aInterval";
  public static final String REPLICATION_INTERVAL = "rInterval";
  public static final String NORMALIZING_CONSTANT = "nconstant";
  public static final String NAME_SERVER_VOTE_SIZE = "nsVoteSize";
  public static final String MIN_REPLICA = "minReplica";
  public static final String MAX_REPLICA = "maxReplica";
  public static final String MAX_REQ_RATE = "maxReqRate";
  public static final String MOVING_AVERAGE_WINDOW_SIZE = "mavg";

  public static final String BEEHIVE_C = "C";
  public static final String BASE = "base";
  public static final String ALPHA = "alpha";

  // other parameters
  public static final String WORKER_THREAD_COUNT = "workerThreadCount";

  public static final String DATA_STORE = "dataStore";
  public static final String MONGO_PORT = "mongoPort";

  // logging related
  public static final String DEBUG_MODE = "debugMode";

  public static final String FILE_LOGGING_LEVEL = "fileLoggingLevel";
  public static final String CONSOLE_OUTPUT_LEVEL = "consoleOutputLevel";
  public static final String STAT_FILE_LOGGING_LEVEL = "statFileLoggingLevel";
  public static final String STAT_CONSOLE_OUTPUT_LEVEL = "statConsoleOutputLevel";

  // paxos parameters
  public static final String PAXOS_LOG_FOLDER = "paxosLogFolder";
  public static final String FAILURE_DETECTION_MSG_INTERVAL = "failureDetectionMsgInterval";
  public static final String FAILURE_DETECTION_TIMEOUT_INTERVAL = "failureDetectionTimeoutInterval";

  // Test-related parameters
  public static final String EXPERIMENT_MODE = "experimentMode";
  public static final String SINGLE_NS = "singleNS";
  public static final String DUMMY_GNS = "dummyGNS";
  public static final String READ_COORDINATION = "readCoordination";
  public static final String EMULATE_PING_LATENCIES = "emulatePingLatencies";
  public static final String VARIATION = "variation";
  public static final String NO_PAXOS_LOG = "noPaxosLog";
  public static final String EVENTUAL_CONSISTENCY = "eventualConsistency";
  public static final String MULTI_PAXOS = "multipaxos";
  public static final String HELP_HEADER = "NOTE: Options whose description starts with [EXP] are needed only during " +
          "experiments and can be ignored otherwise.";
  public static final String HELP_FOOTER = "";

  /**
   * Returns the list of command line options recognized by a name servers.
   * Method returns a {@link org.apache.commons.cli.Options} object which includes all command line options.
   */
  public static Options getAllOptions() {
    Option help = new Option(HELP, "Prints usage");
    Option configFile = new Option(CONFIG_FILE, true, "Configuration file with list of parameters and values (an alternative to using command-line options)");
    Option staticReplication = new Option(STATIC, "Replicate name at fixed set of name servers");
    Option randomReplication = new Option(RANDOM, "[EXP] Randomly select new active name servers");
    Option locationBasedReplication = new Option(LOCATION, "Locality-based selection of active name serervs for a name (Default replication policy)");
    Option beehiveReplication = new Option(BEEHIVE, "[EXP]  Beehive replication");
    OptionGroup replication = new OptionGroup().addOption(staticReplication)
            .addOption(randomReplication)
            .addOption(locationBasedReplication)
            .addOption(beehiveReplication);

    Option aggregateInterval = new Option(AGGREGATE_INTERVAL, true, "Interval between collecting stats");
    aggregateInterval.setArgName("seconds");

    Option replicationInterval = new Option(REPLICATION_INTERVAL, true, "Interval between computing new replicas");
    aggregateInterval.setArgName("seconds");


    Option normalizingConstant = new Option(NORMALIZING_CONSTANT, true, "Constant for deciding number of replicas as follows. #replicas = (read rate)/(write rate)/(nconstant)");
    normalizingConstant.setArgName("#");

    Option maxReqRate = new Option(MAX_REQ_RATE, true, "Maximum request rate that a name server can sustain");

    Option workerThreadCount = new Option(WORKER_THREAD_COUNT, true, "Number of worker threads");

    Option debugMode = new Option(DEBUG_MODE, "Debug mode to print more verbose logs. Set to true only if log level is FINE or more verbose.");
    Option experimentMode = new Option(EXPERIMENT_MODE, "[EXP] Run in experiment mode. May execute some code that is needed only during experiments");

    Option eventualConsistency = new Option(EVENTUAL_CONSISTENCY, "[EXP] Eventual consistency or paxos");

    Option dataStore = new Option(DATA_STORE, true, "Persistent data store to use for name records");

    Option mongoPort = new Option(MONGO_PORT, true, "Port number to use for MongoDB.");

    Option fileLoggingLevel = new Option(FILE_LOGGING_LEVEL, true, "Verbosity level of log file");
    Option consoleOutputLevel = new Option(CONSOLE_OUTPUT_LEVEL, true, "Verbosity level of console output");
    Option statFileLoggingLevel = new Option(STAT_FILE_LOGGING_LEVEL, true, "Verbosity level of log file for experiment related statistics");
    Option statConsoleOutputLevel = new Option(STAT_CONSOLE_OUTPUT_LEVEL, true, "Verbosity level of console output for experiment related statistics");

    Option nodeId = new Option(ID, true, "Node ID");
    nodeId.setArgName("NodeID");

    Option nsFile = new Option(NS_FILE, true, "File with node configuration of all name servers");
    nsFile.setArgName("file");

    Option localNsCount = new Option(LOCAL_NS_COUNT, true, "Number of name servers to create on a local hosts");
    localNsCount.setArgName("file");

    Option localLnsCount = new Option(LOCAL_LNS_COUNT, true, "Number of local name servers to create on a local host");
    localNsCount.setArgName("file");

    Option primaryReplicas = new Option(PRIMARY_REPLICAS, true, "Number of replica controllers for a name");
    primaryReplicas.setArgName("#primaries");

    Option paxosLogFolder = new Option(PAXOS_LOG_FOLDER, true, "Folder where paxos logs are stored in a persistent manner.");
    Option failureDetectionMsgInterval = new Option(FAILURE_DETECTION_MSG_INTERVAL, true,
            "Interval (in sec) between two failure detection messages sent to a node");
    Option failureDetectionTimeoutInterval = new Option(FAILURE_DETECTION_TIMEOUT_INTERVAL, true,
            "Interval (in sec) after which a node is declared as failed");

    Option movingAverageWindowSize = new Option(MOVING_AVERAGE_WINDOW_SIZE, true, "Size of window to calculate the "
            + "moving average of update inter-arrival time");
    movingAverageWindowSize.setArgName("size");


    Option nameServerVoteSize = new Option(NAME_SERVER_VOTE_SIZE, true, "Maximum number of replicas selected based on locality");
    nameServerVoteSize.setArgName("count");

    Option minReplica = new Option(MIN_REPLICA, true, "Minimum number of replicas");
    Option maxReplica = new Option(MAX_REPLICA, true, "Maximum number of replicas");

    Option C = new Option(BEEHIVE_C, true, "[EXP] Beehive average hop count");
    C.setArgName("hop");

    Option base = new Option(BASE, true, "[EXP] Beehive base of DHT");
    base.setArgName("#");

    Option alpha = new Option(ALPHA, true, "[EXP] Beehive zipf distribution parameter");
    alpha.setArgName("#");

    // used for testing only
    Option singleNS = new Option(SINGLE_NS, false, "Run an un-replicated single name server");
    Option readCoordination = new Option(READ_COORDINATION, false, "[EXP] Coordinate with all replicas of a name on read requests as well");
    Option emulatePingLatencies = new Option(EMULATE_PING_LATENCIES, "[EXP] Emulate a packet delay equal to ping delay in between two servers");
    Option variation = new Option(VARIATION, true, "[EXP] During emulation, what fraction of random variation to add to delay");
    Option noPaxosLog = new Option(NO_PAXOS_LOG, false, "[EXP] Do not create paxos logs (supported by paxos package, not by multipaxos package)");
    Option multipaxos = new Option(MULTI_PAXOS, false, "Use multipaxos package (otherwise paxos package is used)");
    Option dummyGNS = new Option(DUMMY_GNS, false, "[EXP] Use a dummy GNS app instead of actual GNS");

    Options commandLineOptions = new Options();
    commandLineOptions.addOption(configFile);
    commandLineOptions.addOption(nodeId);
    commandLineOptions.addOption(nsFile);
    commandLineOptions.addOption(localNsCount);
    commandLineOptions.addOption(localLnsCount);
    commandLineOptions.addOption(primaryReplicas);

    commandLineOptions.addOptionGroup(replication);

    commandLineOptions.addOption(aggregateInterval);
    commandLineOptions.addOption(replicationInterval);
    commandLineOptions.addOption(normalizingConstant);
    commandLineOptions.addOption(nameServerVoteSize);
    commandLineOptions.addOption(minReplica);
    commandLineOptions.addOption(maxReplica);
    commandLineOptions.addOption(maxReqRate);
    commandLineOptions.addOption(C);
    commandLineOptions.addOption(base);
    commandLineOptions.addOption(alpha);

    commandLineOptions.addOption(movingAverageWindowSize);

    commandLineOptions.addOption(debugMode);
    commandLineOptions.addOption(experimentMode);
    commandLineOptions.addOption(eventualConsistency);
    commandLineOptions.addOption(dataStore);
    commandLineOptions.addOption(mongoPort);

    commandLineOptions.addOption(paxosLogFolder);
    commandLineOptions.addOption(failureDetectionMsgInterval);
    commandLineOptions.addOption(failureDetectionTimeoutInterval);
    commandLineOptions.addOption(emulatePingLatencies);
    commandLineOptions.addOption(variation);

    commandLineOptions.addOption(workerThreadCount);
    commandLineOptions.addOption(help);

    commandLineOptions.addOption(fileLoggingLevel);
    commandLineOptions.addOption(consoleOutputLevel);
    commandLineOptions.addOption(statConsoleOutputLevel);
    commandLineOptions.addOption(statFileLoggingLevel);
    commandLineOptions.addOption(singleNS);
    commandLineOptions.addOption(readCoordination);
    commandLineOptions.addOption(noPaxosLog);
    commandLineOptions.addOption(multipaxos);
    commandLineOptions.addOption(dummyGNS);
    return commandLineOptions;
  }
}
