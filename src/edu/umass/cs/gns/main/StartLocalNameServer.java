package edu.umass.cs.gns.main;

import edu.umass.cs.gns.localnameserver.LocalNameServer;
import edu.umass.cs.gns.nsdesign.GNSNodeConfig;
import edu.umass.cs.gns.nsdesign.replicationframework.BeehiveDHTRouting;
import edu.umass.cs.gns.nsdesign.replicationframework.ReplicationFrameworkType;
import edu.umass.cs.gns.util.AdaptiveRetransmission;
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

/**
 * Starts a single instance of the Local Nameserver with the specified parameters.
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
  public static final String USE_GNS_NIO_TRANSPORT = "useGNSNIOTransport";
  public static final String EMULATE_PING_LATENCIES = "emulatePingLatencies";
  public static final String VARIATION = "variation";

  public static ReplicationFrameworkType replicationFramework = ReplicationFrameworkType.LOCATION;

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
  public static long voteIntervalMillis = 100000;
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

  // nio parameters
  /**
   * Use the new nio implementation: nio/GNSNIOTransport.java
   */
  public static boolean useGNSNIOTransport = true;

  /**
   *
   */
  public static boolean emulatePingLatencies = false;
  public static double variation = 0.1; // 10 % variation above latency in config file.

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

    Option fileLoggingLevel = new Option(FILE_LOGGING_LEVEL, true, "fileLoggingLevel");
    Option consoleOutputLevel = new Option(CONSOLE_OUTPUT_LEVEL, true, "consoleOutputLevel");
    Option statFileLoggingLevel = new Option(STAT_FILE_LOGGING_LEVEL, true, "statFileLoggingLevel");
    Option statConsoleOutputLevel = new Option(STAT_CONSOLE_OUTPUT_LEVEL, true, "statConsoleOutputLevel");

    Option useGNSNIOTransport = new Option(USE_GNS_NIO_TRANSPORT, "if true, we use class GNSNIOTransport.java, else use NioServer.java");

    Option emulatePingLatencies = new Option(EMULATE_PING_LATENCIES, "add packet delay equal to ping delay between two servers (used for emulation).");
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
    commandLineOptions.addOption(useGNSNIOTransport);
    commandLineOptions.addOption(emulatePingLatencies);
    commandLineOptions.addOption(variation);

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
   -debugMode
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
    String nsFile = "";  //Nameserver file
    startLNS(id, nsFile, args);
  }


  public static void startLNS(int id, String nsFile, String... args) {
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
      String configFile = null;
      if (parser.hasOption(CONFIG_FILE))
        configFile = parser.getOptionValue(CONFIG_FILE);

      startLNSConfigFile(id, nsFile, configFile, parser);
    }  catch (Exception e1) {
      e1.printStackTrace();
      printUsage();
      System.exit(1);
    }
  }

  /**
   * During testing, we can start LNS by calling this method without using command line parameters.
   * Three parameters of this method, <code>id</code>, <code>nsFile</code>, and <code>configFile</code>, are
   * sufficient to start local name server; <code>CommandLine</code> can be null. Values of id and nsFile will not be
   * used, if config file also contains values of these parameters.
   *
   * @param id  ID of local name server
   * @param nsFile node config file (can be null)
   * @param configFile config file with parameters (can be null)
   * @param parser command line arguments (can be null)
   */
  public static void startLNSConfigFile(int id, String nsFile, String configFile, CommandLine parser) {
    try {

      // create a hash map with all options including options in config file and the command line arguments
      HashMap<String, String> allValues = new HashMap<String, String>();

      if (configFile != null) {
        File f = new File(configFile);
        if (f.exists() == false) {
          System.err.println("Config file not found:" + configFile);
          System.exit(2);
        }
        InputStream input = new FileInputStream(configFile);
        // load a properties file
        Properties prop = new Properties();
        prop.load(input);

        // add options given in config file to hash map
        for (String propertyName: prop.stringPropertyNames()) {
          allValues.put(propertyName, prop.getProperty(propertyName));
        }

      }

      // add options given via command line to hashmap. these options can override options given in config file.
      if (parser!= null) {
        for (Option option: parser.getOptions()) {
          String argName = option.getOpt();
          String value = option.getValue();
          // if an option has a boolean value, the command line arguments do not say true/false for some of these options
          // if option name is given as argument on the command line, it means the value is true. therefore, the hashmap
          // will also assign the value true for these options.
          if (value == null) value = "true";
          allValues.put(argName, value);
        }
      }

      if (allValues.containsKey(ID)) id = Integer.parseInt(allValues.get(ID));
      if (allValues.containsKey(NS_FILE))  nsFile = allValues.get(NS_FILE);

      cacheSize = (allValues.containsKey(CACHE_SIZE)) ? Integer.parseInt(allValues.get(CACHE_SIZE)) : 10000;

      GNS.numPrimaryReplicas = allValues.containsKey(PRIMARY) ?
              Integer.parseInt(allValues.get(PRIMARY)) : GNS.DEFAULT_NUM_PRIMARY_REPLICAS;

      if (allValues.containsKey(LOCATION) && Boolean.parseBoolean(allValues.get(LOCATION))) {
        replicationFramework = ReplicationFrameworkType.LOCATION;
        voteIntervalMillis = Integer.parseInt(allValues.get(VOTE_INTERVAL)) * 1000;
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
      workloadFile = allValues.containsKey(WORKLOAD_FILE) ? allValues.get(WORKLOAD_FILE): null;

      // all parameters related to synthetic workload
      isSyntheticWorkload = allValues.containsKey(ZIPF) && Boolean.parseBoolean(allValues.get(ZIPF));
      alpha = (isSyntheticWorkload) ? Double.parseDouble(allValues.get(ALPHA)) : 0;
      regularWorkloadSize = (isSyntheticWorkload) ? Integer.parseInt(allValues.get(REGULAR_WORKLOAD)) : 0;
      mobileWorkloadSize = (isSyntheticWorkload) ? Integer.parseInt(allValues.get(MOBILE_WORKLOAD)) : 0;

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

      if (allValues.containsKey(USE_GNS_NIO_TRANSPORT)) {
        useGNSNIOTransport = Boolean.parseBoolean(allValues.get(USE_GNS_NIO_TRANSPORT));
      }

      emulatePingLatencies = allValues.containsKey(EMULATE_PING_LATENCIES) &&
              Boolean.parseBoolean(allValues.get(EMULATE_PING_LATENCIES));

      if (emulatePingLatencies && allValues.containsKey(VARIATION)) {
        variation = Double.parseDouble(allValues.get(VARIATION));
      }

    } catch (Exception e1) {
      e1.printStackTrace();
      printUsage();
      System.exit(1);
    }

    DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
    Date date = new Date();
    GNS.getLogger().fine("Date: " + dateFormat.format(date));
    GNS.getLogger().fine("Id: " + id);
    GNS.getLogger().fine("NS File: " + nsFile);
    GNS.getLogger().fine("Regular Workload Size: " + regularWorkloadSize);
    GNS.getLogger().fine("Mobile Workload Size: " + mobileWorkloadSize);
    GNS.getLogger().fine("Workload File: " + workloadFile);
    GNS.getLogger().fine("Lookup Trace File: " + lookupTraceFile);
    GNS.getLogger().fine("Update Trace File: " + updateTraceFile);
    GNS.getLogger().fine("Primary: " + GNS.numPrimaryReplicas);
    GNS.getLogger().fine("Alpha: " + alpha);
    GNS.getLogger().fine("Lookups: " + numQuery);
    GNS.getLogger().fine("Updates: " + numUpdate);
    GNS.getLogger().fine("Lookup Rate: " + lookupRate + "ms");
    GNS.getLogger().fine("Update Rate Mobile: " + updateRateMobile + "ms");
    GNS.getLogger().fine("Update Rate Regular: " + updateRateRegular + "ms");
    GNS.getLogger().fine("Zipf Workload: " + isSyntheticWorkload);
    GNS.getLogger().fine("Replication: " + replicationFramework.toString());
    GNS.getLogger().fine("Vote Interval: " + voteIntervalMillis + "ms");
    GNS.getLogger().fine("Cache Size: " + cacheSize);
    GNS.getLogger().fine("Experiment Mode: " + experimentMode);
    GNS.getLogger().fine("Debug Mode: " + debugMode);

    try {
//      ConfigFileInfo.readHostInfo(nsFile, id);
      GNSNodeConfig gnsNodeConfig = new GNSNodeConfig(nsFile, id);
      ConsistentHashing.initialize(GNS.numPrimaryReplicas, gnsNodeConfig.getNameServerIDs());

      //Start local name server
      new LocalNameServer(id, gnsNodeConfig);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
