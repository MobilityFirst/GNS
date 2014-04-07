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

  public static final String STATIC = "static";

  public static final String RANDOM = "random";

  public static final String LOCATION = "location";

  public static final String BEEHIVE = "beehive";

  public static final String KMEDIODS = "kmediods";

  public static final String OPTIMAL = "optimal";

  public static final String WORKER_THREAD_COUNT = "workerThreadCount";

  public static final String TINY_UPDATE = "tinyUpdate";

  public static final String DEBUG_MODE = "debugMode";

  public static final String EXPERIMENT_MODE = "experimentMode";

  public static final String NO_LOAD_DB = "noLoadDB";

  public static final String EVENTUAL_CONSISTENCY = "eventualConsistency";

  public static final String DATA_STORE = "dataStore";

  public static final String SIMPLE_DISK_STORE = "simpleDiskStore";

  public static final String DATA_FOLDER = "dataFolder";

  public static final String MONGO_PORT = "mongoPort";

  public static final String FILE_LOGGING_LEVEL = "fileLoggingLevel";

  public static final String CONSOLE_OUTPUT_LEVEL = "consoleOutputLevel";

  public static final String STAT_FILE_LOGGING_LEVEL = "statFileLoggingLevel";

  public static final String STAT_CONSOLE_OUTPUT_LEVEL = "statConsoleOutputLevel";

  public static final String REGULAR_WORKLOAD = "rworkload";

  public static final String MOBILE_WORKLOAD = "mworkload";

  public static final String PRIMARY_REPLICAS = "primary";

  public static final String AGGREGATE_INTERVAL = "aInterval";

  public static final String REPLICATION_INTERVAL = "rInterval";

  public static final String NORMALIZING_CONSTANT = "nconstant";

  public static final String PAXOS_LOG_FOLDER = "paxosLogFolder";

  public static final String FAILURE_DETECTION_MSG_INTERVAL = "failureDetectionMsgInterval";

  public static final String FAILURE_DETECTION_TIMEOUT_INTERVAL = "failureDetectionTimeoutInterval";

  public static final String EMULATE_PING_LATENCIES = "emulatePingLatencies";

  public static final String VARIATION = "variation";

  public static final String MOVING_AVERAGE_WINDOW_SIZE = "mavg";

  public static final String TTL_CONSTANT = "ttlconstant";

  public static final String TTL_REGULAR_NAMES = "rttl";

  public static final String TTL_MOBILE_NAMES = "mttl";

  public static final String NAME_SERVER_VOTE_SIZE = "nsVoteSize";

  public static final String MIN_REPLICA = "minReplica";

  public static final String MAX_REPLICA = "maxReplica";

  public static final String BEEHIVE_C = "C";

  public static final String BASE = "base";

  public static final String ALPHA = "alpha";

  public static final String NUMBER_LNS = "numLNS";

  public static final String NS_NS_PING_FILE = "nsnsPingFile";

  public static final String LNS_NS_PING_FILE = "lnsnsPingFile";

  public static final String SIGNATURE_CHECK = "signatureCheck";

  public static final String QUIT_AFTER_TIME = "quitAfterTime";

  public static final String NAME_ACTIVES = "nameActives";

  public static final String SINGLE_NS = "singleNS";

//  public static final String USE_COORDINATION = "coordination";


  /**
   * Returns the list of command line options recognized by a name servers.
   * Method returns a {@link org.apache.commons.cli.Options} object which includes all command line options.
   */
  public static Options getAllOptions(){
    Option help = new Option(HELP, "Prints Usage");
    Option configFile = new Option(CONFIG_FILE, true, "File containing configuration parameters");
    Option staticReplication = new Option(STATIC, "Fixed number of active nameservers per name");
    Option randomReplication = new Option(RANDOM, "Randomly select new active nameservers");
    Option locationBasedReplication = new Option(LOCATION, "Location Based selection of active nameserervs");
    Option beehiveReplication = new Option(BEEHIVE, "Beehive Replication");
    Option kmediodsReplication = new Option(KMEDIODS, "K-Mediods Replication");
    Option optimalReplication = new Option(OPTIMAL, "Optimal Replication");
    OptionGroup replication = new OptionGroup().addOption(staticReplication)
            .addOption(randomReplication)
            .addOption(locationBasedReplication)
            .addOption(beehiveReplication)
            .addOption(kmediodsReplication)
            .addOption(optimalReplication);


    Option workerThreadCount = new Option(WORKER_THREAD_COUNT, true, "Number of worker threads");
    Option tinyUpdate = new Option(TINY_UPDATE, "Use a smaller update packet");


    Option debugMode = new Option(DEBUG_MODE, "Run in debug mode");
    Option experimentMode = new Option(EXPERIMENT_MODE, "Run in experiment mode");
    Option noLoadDB = new Option(NO_LOAD_DB, "Load items in database or not");
    Option eventualConsistency = new Option(EVENTUAL_CONSISTENCY, "Eventual consistency or paxos");

    Option dataStore = new Option(DATA_STORE, true, "Which persistent data store to use for name records");

    //Option persistentDataStore = new Option("persistentDataStore", "Use a persistent data store for name records");
    Option simpleDiskStore = new Option(SIMPLE_DISK_STORE, "Use a simple disk store for name records");
    Option dataFolder = new Option(DATA_FOLDER, true, "dataFolder");
    Option mongoPort = new Option(MONGO_PORT, true, "Which port number to use for MongoDB.");

    Option fileLoggingLevel = new Option(FILE_LOGGING_LEVEL, true, "fileLoggingLevel");
    Option consoleOutputLevel = new Option(CONSOLE_OUTPUT_LEVEL, true, "consoleOutputLevel");
    Option statFileLoggingLevel = new Option(STAT_FILE_LOGGING_LEVEL, true, "statFileLoggingLevel");
    Option statConsoleOutputLevel = new Option(STAT_CONSOLE_OUTPUT_LEVEL, true, "statConsoleOutputLevel");

    Option nodeId = OptionBuilder.withArgName("nodeId").hasArg()
            .withDescription("Node id")
            .create(ID);

    Option nsFile = OptionBuilder.withArgName("file").hasArg()
            .withDescription("Name server file")
            .create(NS_FILE);

    Option regularWorkload = OptionBuilder.withArgName("size").hasArg()
            .withDescription("Regular workload size")
            .create(REGULAR_WORKLOAD);
    Option mobileWorkload = OptionBuilder.withArgName("size").hasArg()
            .withDescription("Mobile workload size")
            .create(MOBILE_WORKLOAD);
    Option syntheticWorkloadSleepTimeBetweenAddingNames =
            new Option("syntheticWorkloadSleepTimeBetweenAddingNames", true, "syntheticWorkloadSleepTimeBetweenAddingNames");

    Option primaryReplicas = OptionBuilder.withArgName("#primaries").hasArg()
            .withDescription("Number of primary nameservers")
            .create(PRIMARY_REPLICAS);

    Option aggregateInterval = OptionBuilder.withArgName("seconds").hasArg()
            .withDescription("Interval between collecting stats")
            .create(AGGREGATE_INTERVAL);

    Option replicationInterval = OptionBuilder.withArgName("seconds").hasArg()
            .withDescription("Interval between replication")
            .create(REPLICATION_INTERVAL);

    Option normalizingConstant = OptionBuilder.withArgName("#").hasArg()
            .withDescription("Normalizing constant")
            .create(NORMALIZING_CONSTANT);

    Option paxosLogFolder = new Option(PAXOS_LOG_FOLDER, true, "Folder where paxos logs are stored in a persistent manner.");
    Option failureDetectionMsgInterval = new Option(FAILURE_DETECTION_MSG_INTERVAL, true,
            "Interval (in sec) between two failure detection messages sent to a node");
    Option failureDetectionTimeoutInterval = new Option(FAILURE_DETECTION_TIMEOUT_INTERVAL, true,
            "Interval (in sec) after which a node is declared as failed");

    Option paxosStartMinDelaySec = new Option("paxosStartMinDelaySec", true, "paxos starts at least this many seconds after start of experiment");

    Option paxosStartMaxDelaySec = new Option("paxosStartMaxDelaySec", true, "paxos starts at most this many seconds after start of experiment");

    Option emulatePingLatencies = new Option(EMULATE_PING_LATENCIES, "add packet delay equal to ping delay between two servers (used for emulation).");
    Option variation = new Option(VARIATION, true, "variation");

    Option movingAverageWindowSize = OptionBuilder.withArgName("size").hasArg()
            .withDescription("Size of window to calculate the "
                    + "moving average of update inter-arrival time")
            .create(MOVING_AVERAGE_WINDOW_SIZE);

    Option ttlConstant = OptionBuilder.withArgName("#").hasArg()
            .withDescription("TTL constant")
            .create(TTL_CONSTANT);
    Option defaultTTLRegularNames = OptionBuilder.withArgName("#seconds").hasArg()
            .withDescription("Default TTL value for regular names")
            .create(TTL_REGULAR_NAMES);
    Option defaultTTLMobileNames = OptionBuilder.withArgName("#seconds").hasArg()
            .withDescription("Default TTL value for mobile names")
            .create(TTL_MOBILE_NAMES);
    Option nameServerVoteSize = OptionBuilder.withArgName("size").hasArg()
            .withDescription("Size of name server selection vote")
            .create(NAME_SERVER_VOTE_SIZE);

    Option minReplica = new Option(MIN_REPLICA, true, "Minimum number of replica");
    Option maxReplica = new Option(MAX_REPLICA, true, "Maximum number of replica");

    Option C = OptionBuilder.withArgName("hop").hasArg()
            .withDescription("Average hop count")
            .create(BEEHIVE_C);
    Option base = OptionBuilder.withArgName("#").hasArg()
            .withDescription("Base of DHT")
            .create(BASE);
    Option alpha = OptionBuilder.withArgName("#").hasArg()
            .withDescription("Zipf distribution")
            .create(ALPHA);

    Option numberLNS = OptionBuilder.withArgName("size").hasArg()
            .withDescription("Number of LNS")
            .create(NUMBER_LNS);
    Option nsnsPingFile = OptionBuilder.withArgName("file").hasArg()
            .withDescription("File with all NS-NS ping latencies")
            .create(NS_NS_PING_FILE);
    Option lnsnsPingFile = OptionBuilder.withArgName("file").hasArg()
            .withDescription("File with all LNS-NS ping latencies")
            .create(LNS_NS_PING_FILE);
    Option signatureCheck = new Option(SIGNATURE_CHECK, "whether an update operation checks signature or not");
    // used for testing only

    Option quitAfterTime = new Option(QUIT_AFTER_TIME, true, "name server will quit after this time");

    Option nameActives = new Option(NAME_ACTIVES, false, "list of name actives provided to this file");

    Option singleNS = new Option(SINGLE_NS, false, "If true, run a single name server");

    Options commandLineOptions = new Options();
    commandLineOptions.addOption(configFile);
    commandLineOptions.addOption(nodeId);
    commandLineOptions.addOption(nsFile);
    commandLineOptions.addOption(regularWorkload);
    commandLineOptions.addOption(mobileWorkload);
    commandLineOptions.addOption(syntheticWorkloadSleepTimeBetweenAddingNames);
    commandLineOptions.addOption(primaryReplicas);
    commandLineOptions.addOptionGroup(replication);
    commandLineOptions.addOption(aggregateInterval);
    commandLineOptions.addOption(replicationInterval);
    commandLineOptions.addOption(normalizingConstant);

    commandLineOptions.addOption(movingAverageWindowSize);
    commandLineOptions.addOption(ttlConstant);
    commandLineOptions.addOption(defaultTTLRegularNames);
    commandLineOptions.addOption(defaultTTLMobileNames);

    commandLineOptions.addOption(nameServerVoteSize);
    commandLineOptions.addOption(minReplica);
    commandLineOptions.addOption(maxReplica);
    commandLineOptions.addOption(C);
    commandLineOptions.addOption(base);
    commandLineOptions.addOption(alpha);
    commandLineOptions.addOption(debugMode);
    commandLineOptions.addOption(experimentMode);
    commandLineOptions.addOption(noLoadDB);
    commandLineOptions.addOption(eventualConsistency);
    commandLineOptions.addOption(dataStore);
    commandLineOptions.addOption(simpleDiskStore);
    commandLineOptions.addOption(dataFolder);
    commandLineOptions.addOption(mongoPort);

    commandLineOptions.addOption(paxosLogFolder);
    commandLineOptions.addOption(failureDetectionMsgInterval);
    commandLineOptions.addOption(failureDetectionTimeoutInterval);
    commandLineOptions.addOption(paxosStartMinDelaySec);
    commandLineOptions.addOption(paxosStartMaxDelaySec);
    commandLineOptions.addOption(emulatePingLatencies);
    commandLineOptions.addOption(variation);

    commandLineOptions.addOption(workerThreadCount);
    commandLineOptions.addOption(tinyUpdate);
    commandLineOptions.addOption(help);

    commandLineOptions.addOption(numberLNS);
    commandLineOptions.addOption(nsnsPingFile);
    commandLineOptions.addOption(lnsnsPingFile);


    commandLineOptions.addOption(fileLoggingLevel);
    commandLineOptions.addOption(consoleOutputLevel);
    commandLineOptions.addOption(statConsoleOutputLevel);
    commandLineOptions.addOption(statFileLoggingLevel);
    commandLineOptions.addOption(signatureCheck);
    commandLineOptions.addOption(quitAfterTime);
    commandLineOptions.addOption(nameActives);
    commandLineOptions.addOption(singleNS);

    return commandLineOptions;
  }
}
