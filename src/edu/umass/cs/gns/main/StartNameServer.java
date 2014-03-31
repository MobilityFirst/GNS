package edu.umass.cs.gns.main;


import edu.umass.cs.gns.nameserver.NameServer;
import edu.umass.cs.gns.test.FailureScenario;
import edu.umass.cs.gns.util.ConfigFileInfo;
import edu.umass.cs.gns.util.ConsistentHashing;
import org.apache.commons.cli.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Properties;

import static edu.umass.cs.gns.util.Util.println;
/**************** FIXME Package deprecated by nsdesign/StartNameServer.java. this will soon be deleted. **/
/**
 * 
 * Starts a single instance of the Nameserver with the specified parameters.
 *           @deprecated
 */
public class StartNameServer {

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

  public static final String USE_GNS_NIO_TRANSPORT = "useGNSNIOTransport";

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



//  public static String configFile;

  public static ReplicationFrameworkType replicationFramework;
  public static int regularWorkloadSize;
  public static int mobileWorkloadSize;
  public static double C;
  public static double base;
  public static double alpha;
  public static long aggregateInterval;
  public static long analysisInterval;
  public static double normalizingConstant;
  public static int movingAverageWindowSize;
  public static double ttlConstant;
  public static int TTLRegularName;
  public static int TTLMobileName;
  public static int nameServerVoteSize;
  //public static boolean isPlanetlab = false;
  public static boolean debugMode = false;
  public static DataStoreType dataStore = DataStoreType.MONGO;
  public static int mongoPort = -1;
  public static boolean simpleDiskStore = true;
  // incore with disk backup
  public static String dataFolder = "/state/partition1/";
  // is in experiment mode: abhigyan.
  // use this flag to make changes to code which will only run during experiments.
  public static boolean experimentMode = false;
  public static boolean noLoadDB = false;

  // this option should always be false. we use only for running some experiments.
  // Note: based on the above comment I'm going to ignore any code that uses this variable when I make changes.
  // Don't be surprised if things break when this is set to true. - Westy
  public static boolean eventualConsistency = false;

  public static boolean useGNSNIOTransport = true;
  public static boolean emulatePingLatencies = false;
  public static double variation = 0.1;
  public static int workerThreadCount = 5; // number of worker threads
  public static boolean tinyUpdate = false; // 
  private static HelpFormatter formatter = new HelpFormatter();
  private static Options commandLineOptions;
  public static int numberLNS;
  public static String lnsnsPingFile;
  public static String nsnsPingFile;
  public static int minReplica = 3;
  public static int maxReplica = 100;
  public static String nameActives = null;
  public static int loadMonitorWindow = 5000;

  // in experiment mode, the default paxos coordinator will send prepare message at a random time between
  // paxosStartMinDelaySec  and paxosStartMaxDelaySec
  public static int paxosStartMinDelaySec = 0;
  public static int paxosStartMaxDelaySec = 0;

  public static int quitNodeID = -1; // only for testing.
  public static int quitAfterTimeSec = -1; // only for testing. Name server will quit after this time
  public static FailureScenario failureScenario = FailureScenario.applyActiveNameServersRunning;

  public static String paxosLogFolder;

  public static int failureDetectionPingInterval;

  public static int failureDetectionTimeoutInterval;


  @SuppressWarnings("static-access")
  /**************************************************************
   * Initialized a command line parser
   * ***********************************************************
   */
  private static CommandLine initializeOptions(String[] args) throws ParseException {
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

//    Option primaryPaxos = new Option("primaryPaxos","Whether primaries use paxos between themselves");
//    Option paxosDiskBackup = new Option("paxosDiskBackup", "Whether paxos stores its state to disk");
    Option paxosLogFolder = new Option(PAXOS_LOG_FOLDER, true, "Folder where paxos logs are stored in a persistent manner.");
    Option failureDetectionMsgInterval = new Option(FAILURE_DETECTION_MSG_INTERVAL, true,
            "Interval (in sec) between two failure detection messages sent to a node");
    Option failureDetectionTimeoutInterval = new Option(FAILURE_DETECTION_TIMEOUT_INTERVAL, true,
            "Interval (in sec) after which a node is declared as failed");

    Option paxosStartMinDelaySec = new Option("paxosStartMinDelaySec", true, "paxos starts at least this many seconds after start of experiment");

    Option paxosStartMaxDelaySec = new Option("paxosStartMaxDelaySec", true, "paxos starts at most this many seconds after start of experiment");

    Option useGNSNIOTransport = new Option(USE_GNS_NIO_TRANSPORT, "if true, we use class GNSNIOTransport.java, else use NioServer.java");

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


    commandLineOptions = new Options();
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

    commandLineOptions.addOption(useGNSNIOTransport);
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

    CommandLineParser parser = new GnuParser();
    return parser.parse(commandLineOptions, args);
  }

  /**
   *
   * Prints command line usage
   */
  private static void printUsage() {
    formatter.printHelp("StartNameServer", commandLineOptions);
  }
  /**
   *
   * Main method that starts the name server with the given command line options.
   *
   * @param args Command line arguments
   * @throws ParseException 
   */

  private static final String DEFAULT_CONFIG_FILE = "gns.conf";

  private static final int DEFAULTAGGREGATEINTERVAL = 1000; // seconds
  private static final int DEFAULTANALYSISINTERVAL = 1000;  // seconds
  private static final double DEFAULTNORMALIZINGCONSTANT = 0.1;
  private static final int DEFAULTMOVINGAVERAGEWINDOWSIZE = 20;
  private static final double DEFAULTTTLCONSTANT = 0.0;
  private static final int DEFAULTTTLREGULARNAME = 0;
  private static final int DEFAULTTTLMOBILENAME = 0;
  private static final int DEFAULTREGULARWORKLOADSIZE = 0;
  private static final int DEFAULTMOBILEMOBILEWORKLOADSIZE = 0;
  private static final String DEFAULTPAXOSLOGPATHNAME = "log/paxos_log";
  private static final DataStoreType DEFAULTDATASTORETYPE = DataStoreType.MONGO;
  private static final int DEFAULT_NAMESERVER_VOTE_SIZE = 5;

  /*
   * Sample invocation
   java -Xmx2g -cp ../../build/jars/GNS.jar edu.umass.cs.gns.main.StartNameServer -id 1 -nsfile name-server-info -primary 3
   -aInterval 1000 -rInterval 1000 -nconstant 0.1 -mavg 20 -ttlconstant 0.0 -rttl 0 -mttl 0 -rworkload 0 -mworkload 0 -location -nsVoteSize 5 
   -fileLoggingLevel FINE -consoleOutputLevel INFO -statFileLoggingLevel INFO -statConsoleOutputLevel INFO -dataStore MONGO -debugMode
   *   
   */
  public static void main(String[] args) {
    int id = 0;					//node id
    String nsFile = "";			//Nameserver file
    startNew(id, nsFile, args);
  }


  public static void startNew(int id, String nsFile, String ... args) {
    try {
      CommandLine parser = null;
      try {
        parser = initializeOptions(args);

      } catch (ParseException e) {
        e.printStackTrace();
        printUsage();
        System.exit(1);
        e.printStackTrace();
      }
      if (parser.hasOption(HELP)) {
        printUsage();
        System.exit(1);
      }

      // load options given in config file in a java properties object
      Properties prop = new Properties();
      if (parser.hasOption(CONFIG_FILE))  {
        String value = parser.getOptionValue(CONFIG_FILE);
        File f = new File(value);
        if (f.exists() == false) {
          System.err.println("Config file not found:" + value);
          System.exit(2);
        }
        InputStream input = new FileInputStream(value);
        // load a properties file
        prop.load(input);

      }

      // create a hash map with all options including options in config file and the command line arguments
      HashMap<String, String> allValues = new HashMap<String, String>();

      // add options given in config file to hash map
      for (String propertyName: prop.stringPropertyNames()) {
        allValues.put(propertyName, prop.getProperty(propertyName));
      }

      // add options given via command line to hashmap. these options can override options given in config file.
      for (Option option: parser.getOptions()) {
        String argName = option.getOpt();
        String value = option.getValue();
        // if an option has a boolean value, the command line arguments do not say true/false for some of these options
        // if option name is given as argument on the command line, it means the value is true. therefore, the hashmap
        // will also assign the value true for these options.
        if (value == null) value = "true";
        allValues.put(argName, value);
//        System.out.println("adding: " + argName + "\t" + value);
      }
//      System.out.println("All values: " + allValues);



      id = allValues.containsKey(ID) ? Integer.parseInt(allValues.get(ID)) : id;
      nsFile = allValues.containsKey(NS_FILE) ? allValues.get(NS_FILE) : nsFile;


      GNS.numPrimaryReplicas = allValues.containsKey(PRIMARY_REPLICAS) ? Integer.parseInt(allValues.get(PRIMARY_REPLICAS)) : GNS.DEFAULT_NUM_PRIMARY_REPLICAS;
      aggregateInterval = allValues.containsKey(AGGREGATE_INTERVAL) ? Integer.parseInt(allValues.get(AGGREGATE_INTERVAL)) * 1000 : DEFAULTAGGREGATEINTERVAL * 1000;
      analysisInterval = allValues.containsKey(REPLICATION_INTERVAL) ? Integer.parseInt(allValues.get(REPLICATION_INTERVAL)) * 1000 : DEFAULTANALYSISINTERVAL * 1000;
      normalizingConstant = allValues.containsKey(NORMALIZING_CONSTANT) ? Double.parseDouble(allValues.get(NORMALIZING_CONSTANT)) : DEFAULTNORMALIZINGCONSTANT;
      movingAverageWindowSize = allValues.containsKey(MOVING_AVERAGE_WINDOW_SIZE) ? Integer.parseInt(allValues.get(MOVING_AVERAGE_WINDOW_SIZE)): DEFAULTMOVINGAVERAGEWINDOWSIZE;

      ttlConstant = allValues.containsKey(TTL_CONSTANT) ? Double.parseDouble(allValues.get(TTL_CONSTANT)): DEFAULTTTLCONSTANT;
      TTLRegularName = allValues.containsKey(TTL_REGULAR_NAMES) ? Integer.parseInt(allValues.get(TTL_REGULAR_NAMES)): DEFAULTTTLREGULARNAME;
      TTLMobileName = allValues.containsKey(TTL_MOBILE_NAMES) ? Integer.parseInt(allValues.get(TTL_MOBILE_NAMES)): DEFAULTTTLMOBILENAME;

      regularWorkloadSize = allValues.containsKey(REGULAR_WORKLOAD) ? Integer.parseInt(allValues.get(REGULAR_WORKLOAD)): DEFAULTTTLREGULARNAME;
      mobileWorkloadSize = allValues.containsKey(MOBILE_WORKLOAD) ? Integer.parseInt(allValues.get(MOBILE_WORKLOAD)): DEFAULTTTLMOBILENAME;

      if (allValues.containsKey(STATIC) && Boolean.parseBoolean(allValues.get(STATIC))) {
        replicationFramework = ReplicationFrameworkType.STATIC;
      } else if (allValues.containsKey(RANDOM) && Boolean.parseBoolean(allValues.get(RANDOM))) {
        replicationFramework = ReplicationFrameworkType.RANDOM;
      } else if (allValues.containsKey(LOCATION) && Boolean.parseBoolean(allValues.get(LOCATION))) {
        replicationFramework = ReplicationFrameworkType.LOCATION;
        nameServerVoteSize = Integer.parseInt(allValues.get(NAME_SERVER_VOTE_SIZE));
      } else if (allValues.containsKey(BEEHIVE) && Boolean.parseBoolean(allValues.get(BEEHIVE))) {
        replicationFramework = ReplicationFrameworkType.BEEHIVE;
        C = Double.parseDouble(allValues.get(BEEHIVE_C));
        base = Double.parseDouble(allValues.get(BASE));
        alpha = Double.parseDouble(allValues.get(ALPHA));
      } else if (allValues.containsKey(KMEDIODS) && Boolean.parseBoolean(allValues.get(KMEDIODS))) {
        replicationFramework = ReplicationFrameworkType.KMEDIODS;
        numberLNS = Integer.parseInt(allValues.get(NUMBER_LNS));
        lnsnsPingFile = allValues.get(LNS_NS_PING_FILE);
        nsnsPingFile = allValues.get(NS_NS_PING_FILE);
      } else if (allValues.containsKey(OPTIMAL) && Boolean.parseBoolean(allValues.get(OPTIMAL))) {
        replicationFramework = ReplicationFrameworkType.OPTIMAL;
      } else {
        replicationFramework = GNS.DEFAULT_REPLICATION_FRAMEWORK;
        nameServerVoteSize = DEFAULT_NAMESERVER_VOTE_SIZE;
      }

      if (allValues.containsKey(MIN_REPLICA)) {
        minReplica = Integer.parseInt(allValues.get(MIN_REPLICA));
      }
      if (allValues.containsKey(MAX_REPLICA)) {
        maxReplica = Integer.parseInt(allValues.get(MAX_REPLICA));
      }

      debugMode = allValues.containsKey(DEBUG_MODE) && Boolean.parseBoolean(allValues.get(DEBUG_MODE));
      experimentMode = allValues.containsKey(EXPERIMENT_MODE) && Boolean.parseBoolean(allValues.get(EXPERIMENT_MODE));
      if (experimentMode) {
        eventualConsistency = allValues.containsKey(EVENTUAL_CONSISTENCY) && Boolean.parseBoolean(allValues.get(EVENTUAL_CONSISTENCY));
        noLoadDB = allValues.containsKey(NO_LOAD_DB) && Boolean.parseBoolean(allValues.get(NO_LOAD_DB));
      }

      String dataStoreString = allValues.get(DATA_STORE);
      if (dataStoreString == null) {
        dataStore = DEFAULTDATASTORETYPE;
      } else {
        try {
          dataStore = DataStoreType.valueOf(dataStoreString);
        } catch (IllegalArgumentException e) {
          dataStore = DEFAULTDATASTORETYPE;
        }
      }

      simpleDiskStore = allValues.containsKey(SIMPLE_DISK_STORE) && Boolean.parseBoolean(allValues.get(SIMPLE_DISK_STORE));

      if (simpleDiskStore && allValues.containsKey(DATA_FOLDER)) {
        dataFolder = allValues.get(DATA_FOLDER);
      }
      if (allValues.containsKey(MONGO_PORT)) {
        mongoPort = Integer.parseInt(allValues.get(MONGO_PORT));
      }

      if (allValues.containsKey(PAXOS_LOG_FOLDER)) {
        paxosLogFolder = allValues.get(PAXOS_LOG_FOLDER);
      } else {
        paxosLogFolder = DEFAULTPAXOSLOGPATHNAME;
      }

      if (allValues.containsKey(FAILURE_DETECTION_MSG_INTERVAL)) {
        failureDetectionPingInterval = Integer.parseInt(allValues.get(FAILURE_DETECTION_MSG_INTERVAL)) * 1000;

      }
      if (allValues.containsKey(FAILURE_DETECTION_TIMEOUT_INTERVAL)) {
        failureDetectionTimeoutInterval = Integer.parseInt(allValues.get(FAILURE_DETECTION_TIMEOUT_INTERVAL)) * 1000;
      }

//      if (experimentMode) {
//        paxosStartMinDelaySec = Integer.parseInt(allValues.get("paxosStartMinDelaySec"));
//        paxosStartMaxDelaySec = Integer.parseInt(allValues.get("paxosStartMaxDelaySec"));
//      }

      if (allValues.containsKey(USE_GNS_NIO_TRANSPORT)) {
        useGNSNIOTransport = allValues.containsKey(USE_GNS_NIO_TRANSPORT) && Boolean.parseBoolean(allValues.get(USE_GNS_NIO_TRANSPORT));
      }

      if (allValues.containsKey(EMULATE_PING_LATENCIES)) {
        emulatePingLatencies = allValues.containsKey(EMULATE_PING_LATENCIES) && Boolean.parseBoolean(allValues.get(EMULATE_PING_LATENCIES));
        if (emulatePingLatencies && allValues.containsKey(VARIATION)) {
          variation = Double.parseDouble(allValues.get(VARIATION));
        }
      }

      if (allValues.containsKey(WORKER_THREAD_COUNT)) {
        workerThreadCount = Integer.parseInt(allValues.get(WORKER_THREAD_COUNT));
      }

      tinyUpdate = allValues.containsKey(TINY_UPDATE) && Boolean.parseBoolean(allValues.get(TINY_UPDATE));

      if (allValues.containsKey(FILE_LOGGING_LEVEL)) {
        GNS.fileLoggingLevel = allValues.get(FILE_LOGGING_LEVEL);
      }
      if (allValues.containsKey(CONSOLE_OUTPUT_LEVEL)) {
        GNS.consoleOutputLevel = allValues.get(CONSOLE_OUTPUT_LEVEL);
      }
      if (allValues.containsKey(STAT_FILE_LOGGING_LEVEL)) {
        GNS.statFileLoggingLevel = allValues.get(STAT_FILE_LOGGING_LEVEL);
      }
      if (allValues.containsKey(STAT_CONSOLE_OUTPUT_LEVEL)) {
        GNS.statConsoleOutputLevel = allValues.get(STAT_CONSOLE_OUTPUT_LEVEL);
      }

      // only for testing
      if (experimentMode) {
        if (allValues.containsKey(QUIT_AFTER_TIME)) {
          quitAfterTimeSec = Integer.parseInt(allValues.get(QUIT_AFTER_TIME));
//          if (quitAfterTimeSec >= 0) {
//            Thread t = new Thread() {
//              @Override
//              public void run() {
//                GNS.getLogger().info("Sleeping for " + quitAfterTimeSec + " sec before quitting ...");
//                try {
//                  Thread.sleep(quitAfterTimeSec * 1000);
//                } catch (InterruptedException e) {
//                  e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//                }
//                GNS.getLogger().info("SYSTEM EXIT.");
//                System.exit(2);
//
//              }
//            };
//            t.start();
//          }

        }
      }

      // only for testing
      if (experimentMode) {
        nameActives = allValues.get(NAME_ACTIVES);
      }

//      NSListenerUpdate.doSignatureCheck = parser.hasOption("signatureCheck");
    } catch (Exception e1) {
      printUsage();
      e1.printStackTrace();
      System.exit(1);
    }
    DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
    Date date = new Date();

    println(
            "Date: " + dateFormat.format(date), debugMode);
    println(
            "Id: " + id, debugMode);
    println(
            "NS File: " + nsFile, debugMode);
    println(
            "Data Store: " + dataStore, debugMode);
    println(
            "Regular Workload Size: " + regularWorkloadSize, debugMode);
    println(
            "Mobile Workload Size: " + mobileWorkloadSize, debugMode);
    println(
            "Primary: " + GNS.numPrimaryReplicas, debugMode);
    println(
            "Replication: " + replicationFramework.toString(), debugMode);
    println(
            "C: " + C, debugMode);
    println(
            "DHT Base: " + base, debugMode);
    println(
            "Alpha: " + alpha, debugMode);
    println(
            "Aggregate Interval: " + aggregateInterval + "ms", debugMode);
    println(
            "Replication Interval: " + analysisInterval + "ms", debugMode);
    println(
            "Normalizing Constant: " + normalizingConstant, debugMode);
    println(
            "Moving Average Window: " + movingAverageWindowSize, debugMode);
    println(
            "TTL Constant: " + ttlConstant, debugMode);
    println(
            "Default TTL Regular Names: " + TTLRegularName, debugMode);
    println(
            "Default TTL Mobile Names: " + TTLMobileName, debugMode);
    println(
            "Debug Mode: " + debugMode, debugMode);
    println(
            "Experiment Mode: " + experimentMode, debugMode);

    try {
      //Read host info for all nodes. Do this first because hash function depends on ConfigFileInfo.
      ConfigFileInfo.readHostInfo(nsFile, id);
      // Do this before starting name server. We must initialize hash function to calculate the
      // set of replica controllers (primaries) for a name.
      ConsistentHashing.initialize(GNS.numPrimaryReplicas, ConfigFileInfo.getNumberOfNameServers());

      //Start nameserver
      new NameServer(id);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Method used for testing by failing code at arbitrary failure scenarios.
   * @param failureScenario
   */
  public static void checkFailure(FailureScenario failureScenario) {
//    GNS.getLogger().fine("Node\t" + StartNameServer.quitNodeID + "\tFailureScenario\t" + failureScenario.toString());
    if (NameServer.getNodeID() == StartNameServer.quitNodeID && failureScenario.equals(StartNameServer.failureScenario)) {
      GNS.getLogger().severe("SYSTEM EXIT. Failure Scenario. " + failureScenario);
      System.exit(2);
    }
  }
}
