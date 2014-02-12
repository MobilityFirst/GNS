package edu.umass.cs.gns.main;

import edu.umass.cs.gns.localnameserver.original.LocalNameServer;
import edu.umass.cs.gns.replicationframework.BeehiveDHTRouting;
import edu.umass.cs.gns.util.AdaptiveRetransmission;
import edu.umass.cs.gns.util.ConfigFileInfo;
import edu.umass.cs.gns.util.HashFunction;
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

/**
 * ************************************************************
 * Starts a single instance of the Local Nameserver with the specified parameters.
 *
 * @author Hardeep Uppal ***********************************************************
 */
public class StartLocalNameServer {

  public static final String HELP = "help";

  public static final String CONFIG_FILE = "configFile";

  public static final String ID = "id";

  public static final String NS_FILE = "nsfile";

  public static final String CACHE_SIZE = "cacheSize";

  public static final String PRIMARY = "primary";

  public static final String LOCATION = "location";

  public static final String VOTE_INTERVAL = "vInterval";

  public static final String BEEHIVE = "beehive";

  public static final String BEEHIVE_BASE = "beehiveBase";

  public static final String LEAF_SET = "leafSet";

  public static final String OPTIMAL = "optimal";

  public static final String OPTIMAL_TRACE = "optimalTrace";

  public static final String REPLICATION_INTERVAL = "rInterval";

  public static final String LOAD_DEPENDENT_REDIRECTION = "loadDependentRedirection";

  public static final String LOAD_MONITOR_INTERVAL = "nsLoadMonitorIntervalSeconds";

  public static final String MAX_QUERY_WAIT_TIME = "maxQueryWaitTime";

  public static final String NUMBER_OF_TRANSMISSIONS = "numberOfTransmissions";

  public static final String QUERY_TIMEOUT = "queryTimeout";

  public static final String ADAPTIVE_TIMEOUT = "adaptiveTimeout";

  public static final String DELTA = "delta";

  public static final String MU = "mu";

  public static final String PHI = "phi";

  public static final String ZIPF = "zipf";

  public static final String ALPHA = "alpha";

  public static final String REGULAR_WORKLOAD = "rworkload";

  public static final String MOBILE_WORKLOAD = "mworkload";

  public static final String WORKLOAD_FILE = "wfile";

  public static final String LOOKUP_TRACE = "lookupTrace";

  public static final String UPDATE_TRACE = "updateTrace";

  public static final String NUM_QUERY = "numQuery";

  public static final String NUM_UPDATE = "numUpdate";

  public static final String NAME = "name";

  public static final String LOOKUP_RATE = "lookupRate";

  public static final String UPDATE_RATE_MOBILE = "updateRateMobile";

  public static final String UPDATE_RATE_REGULAR = "updateRateRegular";

  public static final String OUTPUT_SAMPLE_RATE = "outputSampleRate";

  public static final String DEBUG_MODE = "debugMode";

  public static final String EXPERIMENT_MODE = "experimentMode";

  public static final String FILE_LOGGING_LEVEL = "fileLoggingLevel";

  public static final String CONSOLE_OUTPUT_LEVEL = "consoleOutputLevel";

  public static final String STAT_FILE_LOGGING_LEVEL = "statFileLoggingLevel";

  public static final String STAT_CONSOLE_OUTPUT_LEVEL = "statConsoleOutputLevel";

  public static final String TINY_QUERY = "tinyQuery";

  public static final String EMULATE_PING_LATENCIES = "emulatePingLatencies";

  public static final String VARIATION = "variation";

  public static final String RUN_HTTP_SERVER = "runHttpServer";


  public static ReplicationFrameworkType replicationFramework;
  public static int regularWorkloadSize;
  public static int mobileWorkloadSize;
  public static double alpha;
  /**
   * A list of names that are queried from the local name server *
   */
  public static String workloadFile;
  public static String lookupTraceFile;
  public static String updateTraceFile;
  public static int numQuery;
  public static int numUpdate;
  public static long voteIntervalMillis = 1000;
  public static boolean isSyntheticWorkload = false;
  public static String name;
  public static int cacheSize = 1000;
  public static boolean debugMode = false;
  private static HelpFormatter formatter = new HelpFormatter();
  private static Options commandLineOptions;
  public static double lookupRate;
  public static double updateRateMobile;
  public static double updateRateRegular;
  public static String optimalTrace = null;
  public static int replicationInterval = 0;
  public static double outputSampleRate = 1.0;
//  /**
//   * Option not used currently.
//   * Variant of voting, in which votes are sent not for closest but for a random node among k-closest.
//   * Name server chosen for a name is the (name modulo k)-th closest node.
//   */
//  public static int chooseFromClosestK = 1;

  /**
   * Used for running experiments for Auspice paper.
   */
  public static boolean experimentMode = false;
  public static boolean noEmail = false;
  /**
   * Used for running experiments with replicate everywhere strategy.
   */
  public static boolean replicateAll = false;

//  Abhigyan: parameters related to retransmissions.
//  If adaptive timeouts are used, see more parameters in util.AdaptiveRetransmission.java

  /**
   *
   * Maximum number of transmission of a query *
   */
  public static int numberOfTransmissions = GNS.DEFAULT_NUMBER_OF_TRANSMISSIONS;

  /**
   * Maximum time a local name server waits for a response from name server query is logged as failed after this.
   */
  public static int maxQueryWaitTime = GNS.DEFAULT_MAX_QUERY_WAIT_TIME;

  /**
   * Fixed timeout after which a query retransmitted.
   */
  public static int queryTimeout = GNS.DEFAULT_QUERY_TIMEOUT;

  /**
   * Whether use a fixed timeout or an adaptive timeout. By default, fixed timeout is used.
   */
  public static boolean adaptiveTimeout = false;

  // Abhigyan: parameters related to server load balancing.
  /**
   * Should local name server do load dependent redirection.
   */
  public static boolean loadDependentRedirection = false;

  /**
   * Frequency at which lns queries all name servers for load valus
   */
  public static int nameServerLoadMonitorIntervalSeconds = 180;

  /**
   * Requests are not sent to servers whose load is above this threshold.
   */
  public static double serverLoadThreshold = 5.0; //

  public static boolean tinyQuery = false;

  /**
   *
   */
  public static boolean emulatePingLatencies = false;
  public static double variation = 0.1; // 10 % addition
  public static boolean runHttpServer = true;

  @SuppressWarnings("static-access")
  /**
   * ************************************************************
   * Initialized a command line parser ***********************************************************
   */
  private static CommandLine initializeOptions(String[] args) throws ParseException {
    Option help = new Option("help", "Prints Usage");

    Option configFile = new Option(CONFIG_FILE, true, "Config file with all parameters");

    Option debugMode = new Option(DEBUG_MODE, "Run in debug mode");
    Option experimentMode = new Option(EXPERIMENT_MODE, "Mode to run experiments for Auspice paper");


    Option syntheticWorkload = new Option(ZIPF, "Use Zipf distribution to generate worklaod");

    Option locationBasedReplication = new Option(LOCATION, "Location Based selection of active nameserervs");
    Option optimalReplication = new Option(OPTIMAL, "Optimal replication");
    Option beehiveReplication = new Option(BEEHIVE, "Beehive replication");
    OptionGroup replication = new OptionGroup()
            .addOption(locationBasedReplication)
            .addOption(beehiveReplication)
            .addOption(optimalReplication);

    Option beehiveDHTbase = new Option(BEEHIVE_BASE, true, "Beehive DHT base, default 16");
    Option beehiveLeafset = new Option(LEAF_SET, true, "Beehive Leaf set size, must be less thant number of name servers, default 24");

    Option loadDependentRedirection = new Option(LOAD_DEPENDENT_REDIRECTION, "local name servers start load balancing among name servers");
    Option nsLoadMonitorIntervalSeconds = new Option(LOAD_MONITOR_INTERVAL, true, "interval of monitoring load at every nameserver (seconds)");

    Option maxQueryWaitTime = new Option(MAX_QUERY_WAIT_TIME, true, "maximum  Wait Time before query is  declared failed (milli-seconds)");
    Option numberOfTransmissions = new Option(NUMBER_OF_TRANSMISSIONS, true, "maximum number of times a query is transmitted.");
    Option queryTimeout = new Option(QUERY_TIMEOUT, true, "query timeout interval (milli-seconds)");
    Option adaptiveTimeout = new Option(ADAPTIVE_TIMEOUT, "Whether to use an adaptive timeout or a fixed timeout");
    Option delta = new Option(DELTA, true, "Adaptive Retransmission: Weight assigned to latest sample in calculating moving average.");
    Option mu = new Option(MU, true, "Adaptive Retransmission: Co-efficient of estimated RTT in calculating timeout.");
    Option phi = new Option(PHI, true, "Adaptive Retransmission: Co-efficient of deviation in calculating timeout.");

//    Option chooseFromClosestK = new Option("chooseFromClosestK", true, "chooseFromClosestK");

    Option fileLoggingLevel = new Option(FILE_LOGGING_LEVEL, true, "fileLoggingLevel");
    Option consoleOutputLevel = new Option(CONSOLE_OUTPUT_LEVEL, true, "consoleOutputLevel");
    Option statFileLoggingLevel = new Option(STAT_FILE_LOGGING_LEVEL, true, "statFileLoggingLevel");
    Option statConsoleOutputLevel = new Option(STAT_CONSOLE_OUTPUT_LEVEL, true, "statConsoleOutputLevel");

    Option tinyQuery = new Option(TINY_QUERY, "tiny query mode");
    Option delayScheduling = new Option(EMULATE_PING_LATENCIES, "add packet delay equal to ping delay between two servers (used for emulation).");
    Option variation = new Option(VARIATION, true, "variation");

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

    Option workloadFile = OptionBuilder.withArgName("file").hasArg()
            .withDescription("List of names that are queried by the local name server")
            .create(WORKLOAD_FILE);

    Option lookupTraceFile = OptionBuilder.withArgName("file").hasArg()
            .withDescription("Lookup Trace")
            .create(LOOKUP_TRACE);

    Option updateTraceFile = OptionBuilder.withArgName("file").hasArg()
            .withDescription("Update Trace")
            .create(UPDATE_TRACE);

    Option outputSampleRate = new Option(OUTPUT_SAMPLE_RATE, true,
            "fraction of requests whose response time will be sampled");

    Option primaryReplicas = OptionBuilder.withArgName("#primaries").hasArg()
            .withDescription("Number of primary nameservers")
            .create(PRIMARY);

    Option alpha = OptionBuilder.withArgName("#").hasArg()
            .withDescription("Value of alpha in the Zipf Distribution")
            .create(ALPHA);

    Option numLookups = OptionBuilder.withArgName("#").hasArg()
            .withDescription("Number of lookups")
            .create(NUM_QUERY);
    Option numUpdates = OptionBuilder.withArgName("#").hasArg()
            .withDescription("Number of Updates")
            .create(NUM_UPDATE);

    Option name = OptionBuilder.withArgName("name").hasArg()
            .withDescription("Name of host/domain/device queried")
            .create(NAME);

    Option cacheSize = OptionBuilder.withArgName("#").hasArg()
            .withDescription("Size of cache at the local name server"
            + ".Default 1000")
            .create(CACHE_SIZE);

    Option voteInterval = OptionBuilder.withArgName("seconds").hasArg()
            .withDescription("Interval between nameserver votes")
            .create(VOTE_INTERVAL);

    Option lookupRate = OptionBuilder.withArgName("ms").hasArg()
            .withDescription("Inter-arrival time between lookups")
            .create(LOOKUP_RATE);

    Option updateRateMobile = OptionBuilder.withArgName("ms").hasArg()
            .withDescription("Inter-arrival time between updates for mobile names")
            .create(UPDATE_RATE_MOBILE);

    Option updateRateRegular = OptionBuilder.withArgName("ms").hasArg()
            .withDescription("Inter-arrival time between updates for regular names")
            .create(UPDATE_RATE_REGULAR);

    Option replicationInverval = OptionBuilder.withArgName("seconds").hasArg()
            .withDescription("Interval between replication")
            .create(REPLICATION_INTERVAL);
    Option optimalTrace = OptionBuilder.withArgName("file").hasArg()
            .withDescription("Optimal trace file")
            .create(OPTIMAL_TRACE);

    Option runHttpServer = new Option("runHttpServer", "run the http server in the same process as local name server.");

    commandLineOptions = new Options();
    commandLineOptions.addOption(configFile);
    commandLineOptions.addOption(nodeId);
    commandLineOptions.addOption(nsFile);
    commandLineOptions.addOption(regularWorkload);
    commandLineOptions.addOption(mobileWorkload);
    commandLineOptions.addOption(workloadFile);
    commandLineOptions.addOption(lookupTraceFile);
    commandLineOptions.addOption(updateTraceFile);
    commandLineOptions.addOption(outputSampleRate);
    commandLineOptions.addOption(primaryReplicas);
    commandLineOptions.addOption(alpha);
    commandLineOptions.addOption(numLookups);
    commandLineOptions.addOption(numUpdates);
    commandLineOptions.addOption(syntheticWorkload);
    commandLineOptions.addOption(name);
    commandLineOptions.addOption(voteInterval);
    commandLineOptions.addOption(cacheSize);
    commandLineOptions.addOption(lookupRate);
    commandLineOptions.addOption(updateRateMobile);
    commandLineOptions.addOption(updateRateRegular);
    commandLineOptions.addOption(replicationInverval);
    commandLineOptions.addOption(optimalTrace);
    commandLineOptions.addOption(debugMode);
    commandLineOptions.addOption(experimentMode);
    commandLineOptions.addOption(help);
    commandLineOptions.addOptionGroup(replication);
    commandLineOptions.addOption(beehiveDHTbase);
    commandLineOptions.addOption(beehiveLeafset);
    commandLineOptions.addOption(loadDependentRedirection);
    commandLineOptions.addOption(nsLoadMonitorIntervalSeconds);
    commandLineOptions.addOption(maxQueryWaitTime);
    commandLineOptions.addOption(numberOfTransmissions);
    commandLineOptions.addOption(queryTimeout);
    commandLineOptions.addOption(adaptiveTimeout);
    commandLineOptions.addOption(delta);
    commandLineOptions.addOption(mu);
    commandLineOptions.addOption(phi);
    commandLineOptions.addOption(fileLoggingLevel);
    commandLineOptions.addOption(consoleOutputLevel);
    commandLineOptions.addOption(statConsoleOutputLevel);
    commandLineOptions.addOption(statFileLoggingLevel);
    commandLineOptions.addOption(tinyQuery);
    commandLineOptions.addOption(delayScheduling);
    commandLineOptions.addOption(variation);
    commandLineOptions.addOption(runHttpServer);

    CommandLineParser parser = new GnuParser();
    return parser.parse(commandLineOptions, args);
  }

  /**
   * ************************************************************
   * Prints command line usage ***********************************************************
   */
  private static void printUsage() {
    formatter.printHelp("StartLocalNameServer", commandLineOptions);
  }

  /*
   * Sample invocation
   * 
   java -Xmx2g -cp ../../build/jars/GNS.jar edu.umass.cs.gns.main.StartLocalNameServer  -id 3 -nsfile name-server-info 
   -cacheSize 10000 -primary 3 -location -vInterval 1000 -chooseFromClosestK 1 -lookupRate 10000 
   -updateRateMobile 0 -updateRateRegular 10000 -numberOfTransmissions 3 -maxQueryWaitTime 100000 -queryTimeout 100
   -fileLoggingLevel FINE -consoleOutputLevel INFO -statFileLoggingLevel INFO -statConsoleOutputLevel INFO
   -runHttpServer -debugMode
   */
  /**
   * ************************************************************
   * Main method that starts the local name server with the given command line options.
   *
   * @param args Command line arguments
   * @throws ParseException ***********************************************************
   */
  public static void main(String[] args) {
    int id = 0;						//node id
    String nsFile = "";
    startNew(id, nsFile, args);
  }//Nameserver file

  public static void start(int id, String nsFile, String... args) {
    try {
      CommandLine parser = null;
      try {
        parser = initializeOptions(args);
      } catch (ParseException e) {
        e.printStackTrace();
        printUsage();
        System.exit(1);
      }
      if (parser.hasOption("help")) {
        printUsage();
        System.exit(1);
      }
      id = Integer.parseInt(parser.getOptionValue("id", Integer.toString(id)));
      nsFile = parser.getOptionValue("nsfile", nsFile);
      cacheSize = (parser.hasOption("cacheSize"))
              ? Integer.parseInt(parser.getOptionValue("cacheSize")) : 1000;
      GNS.numPrimaryReplicas = Integer.parseInt(parser.getOptionValue("primary", Integer.toString(GNS.DEFAULT_NUM_PRIMARY_REPLICAS)));


      if (parser.hasOption("location")) {
        replicationFramework = ReplicationFrameworkType.LOCATION;
        voteIntervalMillis = Integer.parseInt(parser.getOptionValue("vInterval")) * 1000;
//        if (parser.hasOption("chooseFromClosestK")) {
//          chooseFromClosestK = Integer.parseInt(parser.getOptionValue("chooseFromClosestK"));
//        }
      } else if (parser.hasOption("beehive")) {
        replicationFramework = ReplicationFrameworkType.BEEHIVE;
        BeehiveDHTRouting.beehive_DHTbase = Integer.parseInt(parser.getOptionValue("beehiveBase"));
        BeehiveDHTRouting.beehive_DHTleafsetsize = Integer.parseInt(parser.getOptionValue("leafSet"));
      } else if (parser.hasOption("optimal")) {
        replicationFramework = ReplicationFrameworkType.OPTIMAL;
        optimalTrace = parser.getOptionValue("optimalTrace");
        replicationInterval = Integer.parseInt(parser.getOptionValue("rInterval")) * 1000;
      } else {
        replicationFramework = GNS.DEFAULT_REPLICATION_FRAMEWORK;
      }

      loadDependentRedirection = parser.hasOption("loadDependentRedirection");
      nameServerLoadMonitorIntervalSeconds = (loadDependentRedirection) ?
              Integer.parseInt(parser.getOptionValue("nsLoadMonitorIntervalSeconds")) : 60;

      maxQueryWaitTime = (parser.hasOption("maxQueryWaitTime"))
              ? Integer.parseInt(parser.getOptionValue("maxQueryWaitTime")) : GNS.DEFAULT_MAX_QUERY_WAIT_TIME;
      numberOfTransmissions = (parser.hasOption("numberOfTransmissions"))
              ? Integer.parseInt(parser.getOptionValue("numberOfTransmissions")) : GNS.DEFAULT_NUMBER_OF_TRANSMISSIONS;
      queryTimeout = (parser.hasOption("queryTimeout"))
              ? Integer.parseInt(parser.getOptionValue("queryTimeout")) : GNS.DEFAULT_QUERY_TIMEOUT;

      adaptiveTimeout = parser.hasOption("adaptiveTimeout");
      if (adaptiveTimeout) {
        if (parser.hasOption("delta")) {
          AdaptiveRetransmission.delta = Float.parseFloat(parser.getOptionValue("delta"));
        }
        if (parser.hasOption("mu")) {
          AdaptiveRetransmission.mu = Float.parseFloat(parser.getOptionValue("mu"));
        }
        if (parser.hasOption("phi")) {
          AdaptiveRetransmission.phi = Float.parseFloat(parser.getOptionValue("phi"));
        }
      }

//      name = parser.getOptionValue("name");
      isSyntheticWorkload = parser.hasOption("zipf");
      alpha = (isSyntheticWorkload)
              ? Double.parseDouble(parser.getOptionValue("alpha")) : 0;
      regularWorkloadSize = (isSyntheticWorkload)
              ? Integer.parseInt(parser.getOptionValue("rworkload")) : 0;
      mobileWorkloadSize = (isSyntheticWorkload)
              ? Integer.parseInt(parser.getOptionValue("mworkload")) : 0;
      workloadFile = (isSyntheticWorkload)
              ? parser.getOptionValue("wfile") : null;
//      lookupTraceFile = (isSyntheticWorkload) ? parser.getOptionValue("lookupTrace") : null;
//      updateTraceFile = (isSyntheticWorkload) ? parser.getOptionValue("updateTrace") : null;

      lookupTraceFile = parser.hasOption("lookupTrace") ? parser.getOptionValue("lookupTrace") : null;
      updateTraceFile = parser.hasOption("updateTrace") ? parser.getOptionValue("updateTrace") : null;

      // made these optional for non-experimental use
      numQuery = Integer.parseInt(parser.getOptionValue("nlookup", "0"));
      numUpdate = Integer.parseInt(parser.getOptionValue("nUpdate", "0"));
      lookupRate = Double.parseDouble(parser.getOptionValue("lookupRate", "0"));
      updateRateMobile = Double.parseDouble(parser.getOptionValue("updateRateMobile", "0"));
      updateRateRegular = Double.parseDouble(parser.getOptionValue("updateRateRegular", "0"));
      outputSampleRate = Double.parseDouble(parser.getOptionValue("outputSampleRate", "1.0"));

      debugMode = parser.hasOption("debugMode");
      experimentMode = parser.hasOption("experimentMode");

      // Logging related options.
      if (parser.hasOption("fileLoggingLevel")) {
        GNS.fileLoggingLevel = parser.getOptionValue("fileLoggingLevel");
      }
      if (parser.hasOption("consoleOutputLevel")) {
        GNS.consoleOutputLevel = parser.getOptionValue("consoleOutputLevel");
      }
      if (parser.hasOption("statFileLoggingLevel")) {
        GNS.statFileLoggingLevel = parser.getOptionValue("statFileLoggingLevel");
      }
      if (parser.hasOption("statConsoleOutputLevel")) {
        GNS.statConsoleOutputLevel = parser.getOptionValue("statConsoleOutputLevel");
      }

      if (parser.hasOption("tinyQuery")) {
        tinyQuery = parser.hasOption("tinyQuery");
      }

      if (parser.hasOption("emulatePingLatencies")) {
        emulatePingLatencies = parser.hasOption("emulatePingLatencies");
        if (emulatePingLatencies && parser.hasOption("variation")) {
          variation = Double.parseDouble(parser.getOptionValue("variation"));
        }
      }
      if (parser.hasOption("runHttpServer")) {
        runHttpServer = true;
      }
    } catch (Exception e1) {
      e1.printStackTrace();
      printUsage();
      System.exit(1);
    }

    DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
    Date date = new Date();
    println("Date: " + dateFormat.format(date), debugMode);
    println("Id: " + id, debugMode);
    println("NS File: " + nsFile, debugMode);
    println("Regular Workload Size: " + regularWorkloadSize, debugMode);
    println("Mobile Workload Size: " + mobileWorkloadSize, debugMode);
    println("Workload File: " + workloadFile, debugMode);
    println("Lookup Trace File: " + lookupTraceFile, debugMode);
    println("Update Trace File: " + updateTraceFile, debugMode);
    println("Primary: " + GNS.numPrimaryReplicas, debugMode);
    println("Alpha: " + alpha, debugMode);
    println("Lookups: " + numQuery, debugMode);
    println("Updates: " + numUpdate, debugMode);
    println("Lookup Rate: " + lookupRate + "ms", debugMode);
    println("Update Rate Mobile: " + updateRateMobile + "ms", debugMode);
    println("Update Rate Regular: " + updateRateRegular + "ms", debugMode);
    println("Zipf Workload: " + isSyntheticWorkload, debugMode);
//    println("Name: " + name, debugMode);
    println("Replication: " + replicationFramework.toString(), debugMode);
    println("Vote Interval: " + voteIntervalMillis + "ms", debugMode);
    println("Cache Size: " + cacheSize, debugMode);
    println("Experiment Mode: " + experimentMode, debugMode);
    println("Debug Mode: " + debugMode, debugMode);

    try {
      ConfigFileInfo.readHostInfo(nsFile, id);
      HashFunction.initializeHashFunction();
      //Start local name server 
      new LocalNameServer(id).run();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void startNew(int id, String nsFile, String... args) {
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
      }

      id = Integer.parseInt(allValues.get(ID));
      nsFile = allValues.get(NS_FILE);
      cacheSize = (allValues.containsKey(CACHE_SIZE)) ? Integer.parseInt(allValues.get(CACHE_SIZE)) : 10000;

      GNS.numPrimaryReplicas = allValues.containsKey(PRIMARY) ?
              Integer.parseInt(allValues.get(PRIMARY)) : GNS.DEFAULT_NUM_PRIMARY_REPLICAS;

      if (allValues.containsKey(LOCATION) && Boolean.parseBoolean(allValues.get(LOCATION))) {
        replicationFramework = ReplicationFrameworkType.LOCATION;
        voteIntervalMillis = Integer.parseInt(allValues.get(VOTE_INTERVAL)) * 1000;
//        if (allValues.containsKey("chooseFromClosestK")) {
//          chooseFromClosestK = Integer.parseInt(allValues.get("chooseFromClosestK"));
//        }
      } else if (allValues.containsKey(BEEHIVE) && Boolean.parseBoolean(allValues.get(BEEHIVE))) {
        replicationFramework = ReplicationFrameworkType.BEEHIVE;
        BeehiveDHTRouting.beehive_DHTbase = Integer.parseInt(allValues.get(BEEHIVE_BASE));
        BeehiveDHTRouting.beehive_DHTleafsetsize = Integer.parseInt(allValues.get(LEAF_SET));
      } else if (allValues.containsKey(OPTIMAL) && Boolean.parseBoolean(allValues.get(OPTIMAL))) {
        replicationFramework = ReplicationFrameworkType.OPTIMAL;
        optimalTrace = allValues.get(OPTIMAL_TRACE);
        replicationInterval = Integer.parseInt(allValues.get(REPLICATION_INTERVAL)) * 1000;
      } else {
        replicationFramework = GNS.DEFAULT_REPLICATION_FRAMEWORK;
      }

      loadDependentRedirection = allValues.containsKey(LOAD_DEPENDENT_REDIRECTION) &&
              Boolean.parseBoolean(allValues.get(LOAD_DEPENDENT_REDIRECTION));
      nameServerLoadMonitorIntervalSeconds = (loadDependentRedirection) ?
              Integer.parseInt(allValues.get(LOAD_MONITOR_INTERVAL)) : 60;

      maxQueryWaitTime = (allValues.containsKey(MAX_QUERY_WAIT_TIME))
              ? Integer.parseInt(allValues.get(MAX_QUERY_WAIT_TIME)) : GNS.DEFAULT_MAX_QUERY_WAIT_TIME;
      numberOfTransmissions = (allValues.containsKey(NUMBER_OF_TRANSMISSIONS))
              ? Integer.parseInt(allValues.get(NUMBER_OF_TRANSMISSIONS)) : GNS.DEFAULT_NUMBER_OF_TRANSMISSIONS;
      queryTimeout = (allValues.containsKey(QUERY_TIMEOUT))
              ? Integer.parseInt(allValues.get(QUERY_TIMEOUT)) : GNS.DEFAULT_QUERY_TIMEOUT;

      adaptiveTimeout = allValues.containsKey(ADAPTIVE_TIMEOUT)  && Boolean.parseBoolean(allValues.get(ADAPTIVE_TIMEOUT));
      if (adaptiveTimeout) {
        if (allValues.containsKey(DELTA)) {
          AdaptiveRetransmission.delta = Float.parseFloat(allValues.get(DELTA));
        }
        if (allValues.containsKey(MU)) {
          AdaptiveRetransmission.mu = Float.parseFloat(allValues.get(MU));
        }
        if (allValues.containsKey(PHI)) {
          AdaptiveRetransmission.phi = Float.parseFloat(allValues.get(PHI));
        }
      }

      // lookup and update tace files
      lookupTraceFile = allValues.containsKey(LOOKUP_TRACE) ? allValues.get(LOOKUP_TRACE) : null;
      updateTraceFile = allValues.containsKey(UPDATE_TRACE) ? allValues.get(UPDATE_TRACE) : null;

      // all parameters related to synthetic workload
      isSyntheticWorkload = allValues.containsKey(ZIPF) && Boolean.parseBoolean(allValues.get(ZIPF));
      alpha = (isSyntheticWorkload) ? Double.parseDouble(allValues.get(ALPHA)) : 0;
      regularWorkloadSize = (isSyntheticWorkload) ? Integer.parseInt(allValues.get(REGULAR_WORKLOAD)) : 0;
      mobileWorkloadSize = (isSyntheticWorkload) ? Integer.parseInt(allValues.get(MOBILE_WORKLOAD)) : 0;
      workloadFile = (isSyntheticWorkload) ? allValues.get(WORKLOAD_FILE) : null;
      name = (isSyntheticWorkload) ?  allValues.get("name") : null;
      // made these optional for non-experimental use
      numQuery = allValues.containsKey(NUM_QUERY) ? Integer.parseInt(allValues.get(NUM_QUERY)) : 0;
      numUpdate = allValues.containsKey(NUM_UPDATE) ? Integer.parseInt(allValues.get(NUM_UPDATE)) : 0;

      lookupRate = allValues.containsKey(LOOKUP_RATE) ? Double.parseDouble(allValues.get(LOOKUP_RATE)) : 1.0;
      updateRateMobile = allValues.containsKey(UPDATE_RATE_MOBILE) ?
              Double.parseDouble(allValues.get(UPDATE_RATE_MOBILE)) : 0;
      updateRateRegular = allValues.containsKey(UPDATE_RATE_REGULAR) ?
              Double.parseDouble(allValues.get(UPDATE_RATE_REGULAR)) : 0;

      outputSampleRate = allValues.containsKey(OUTPUT_SAMPLE_RATE) ?
              Double.parseDouble(allValues.get(OUTPUT_SAMPLE_RATE)) : 1.0;

      debugMode = allValues.containsKey(DEBUG_MODE) && Boolean.parseBoolean(allValues.get(DEBUG_MODE));
      experimentMode = allValues.containsKey(EXPERIMENT_MODE)  && Boolean.parseBoolean(allValues.get(EXPERIMENT_MODE));

      // Logging related options.
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

      if (allValues.containsKey(TINY_QUERY) && Boolean.parseBoolean(allValues.get(TINY_QUERY))) {
        tinyQuery = allValues.containsKey(TINY_QUERY);
      }

      emulatePingLatencies = allValues.containsKey(EMULATE_PING_LATENCIES) &&
              Boolean.parseBoolean(allValues.get(EMULATE_PING_LATENCIES));

      if (emulatePingLatencies && allValues.containsKey(VARIATION)) {
        variation = Double.parseDouble(allValues.get(VARIATION));
      }

      runHttpServer = allValues.containsKey(RUN_HTTP_SERVER) ? Boolean.parseBoolean(allValues.get(RUN_HTTP_SERVER)) : true;

    } catch (Exception e1) {
      e1.printStackTrace();
      printUsage();
      System.exit(1);
    }

    DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
    Date date = new Date();
    println("Date: " + dateFormat.format(date), debugMode);
    println("Id: " + id, debugMode);
    println("NS File: " + nsFile, debugMode);
    println("Regular Workload Size: " + regularWorkloadSize, debugMode);
    println("Mobile Workload Size: " + mobileWorkloadSize, debugMode);
    println("Workload File: " + workloadFile, debugMode);
    println("Lookup Trace File: " + lookupTraceFile, debugMode);
    println("Update Trace File: " + updateTraceFile, debugMode);
    println("Primary: " + GNS.numPrimaryReplicas, debugMode);
    println("Alpha: " + alpha, debugMode);
    println("Lookups: " + numQuery, debugMode);
    println("Updates: " + numUpdate, debugMode);
    println("Lookup Rate: " + lookupRate + "ms", debugMode);
    println("Update Rate Mobile: " + updateRateMobile + "ms", debugMode);
    println("Update Rate Regular: " + updateRateRegular + "ms", debugMode);
    println("Zipf Workload: " + isSyntheticWorkload, debugMode);
//    println("Name: " + name, debugMode);
    println("Replication: " + replicationFramework.toString(), debugMode);
    println("Vote Interval: " + voteIntervalMillis + "ms", debugMode);
    println("Cache Size: " + cacheSize, debugMode);
    println("Experiment Mode: " + experimentMode, debugMode);
    println("Debug Mode: " + debugMode, debugMode);

    try {
      ConfigFileInfo.readHostInfo(nsFile, id);
      HashFunction.initializeHashFunction();

      //Start local name server
      new LocalNameServer(id).run();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
