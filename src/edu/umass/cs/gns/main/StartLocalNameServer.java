package edu.umass.cs.gns.main;

import edu.umass.cs.gns.localnameserver.LocalNameServer;
import edu.umass.cs.gns.nsdesign.nodeconfig.GNSNodeConfig;
import edu.umass.cs.gns.nsdesign.replicationframework.BeehiveDHTRouting;
import edu.umass.cs.gns.nsdesign.replicationframework.ReplicationFrameworkType;
import edu.umass.cs.gns.util.AdaptiveRetransmission;
import edu.umass.cs.gns.util.ConsistentHashing;
import org.apache.commons.cli.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
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
  public static final String HELP_HEADER = "NOTE: Options whose description starts with [EXP] are needed only during "
          + "experiments and can be ignored otherwise.";
  public static final String HELP_FOOTER = "";
  public static final String CONFIG_FILE = "configFile";
  public static final String ADDRESS = "address";
  public static final String PORT = "port";
  public static final String NS_FILE = "nsfile";
  public static final String DNS_GNS_ONLY = "dnsGnsOnly";
  public static final String CACHE_SIZE = "cacheSize";
  public static final String PRIMARY = "primary";
  public static final String LOCATION = "location";
  public static final String VOTE_INTERVAL = "vInterval";
  public static final String BEEHIVE = "beehive";
  public static final String BEEHIVE_BASE = "beehiveBase";
  public static final String LEAF_SET = "leafSet";
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
  public static final String EMULATE_PING_LATENCIES = "emulatePingLatencies";
  public static final String VARIATION = "variation";

  // size of cache at local name server
  public static int cacheSize = 1000;

  // replication related parameters
  public static ReplicationFrameworkType replicationFramework = ReplicationFrameworkType.LOCATION;

  public static long voteIntervalMillis = 1000000000;

  /**
   * * Set to true to enable all the debugging logging statements.
   */
  public static boolean debuggingEnabled = false;

  /**
   * * Used for running experiments for Auspice paper.
   */
  public static boolean experimentMode = false;

  // parameters related to experiments that are actually used.
  public static String updateTraceFile;  // trace file for all requests (not just updates)
  public static String workloadFile;    // file containing other experiment-related configuration

  // parameters related to experiments. these are not used anymore. I am keeping them here in case needed.
  public static int numQuery;
  public static int numUpdate;
  public static int regularWorkloadSize;
  public static int mobileWorkloadSize;
  public static double alpha;
  public static boolean isSyntheticWorkload = false;
  public static String name;
  public static double outputSampleRate = 1.0;
  public static boolean replicateAll = false;

  public static boolean noEmail = false;

  /**
   * Set to true if you want the DNS server to not lookup records using DNS (will only lookup records in the GNS).
   */
  public static boolean dnsGnsOnly = false;

//  Abhigyan: parameters related to retransmissions.
//  If adaptive timeouts are used, see more parameters in util.AdaptiveRetransmission.java
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
   * emulates ping latency given in config file
   */
  public static boolean emulatePingLatencies = false;
  public static double variation = 0.1; // 10 % variation above latency in config file.

  private static Options commandLineOptions;

  static {
    Option help = new Option("help", "Prints usage");

    Option configFile = new Option(CONFIG_FILE, true, "Config file with all parameters and values (an alternative to command-line options)");

    Option debugMode = new Option(DEBUG_MODE, "Debug mode to print more verbose logs. Set to true only if log level is FINE or more verbose.");
    Option experimentMode = new Option(EXPERIMENT_MODE, "[EXP] Run in experiment mode. May execute some code that is needed only during experiments");

    Option dnsGnsOnly = new Option(DNS_GNS_ONLY, "With this option DNS server only does lookup in GNS server.");

    Option syntheticWorkload = new Option(ZIPF, "[EXP] Use Zipf distribution to generate workload");

    Option locationBasedReplication = new Option(LOCATION, "Locality-based selection of active nameserervs (default option)");
    Option beehiveReplication = new Option(BEEHIVE, "[EXP] Beehive replication");
    OptionGroup replication = new OptionGroup()
            .addOption(locationBasedReplication)
            .addOption(beehiveReplication);

    Option beehiveDHTbase = new Option(BEEHIVE_BASE, true, "[EXP] Beehive DHT base, default 16");
    Option beehiveLeafset = new Option(LEAF_SET, true, "[EXP] Beehive Leaf set size, must be less thant number of name servers, default 24");

    Option loadDependentRedirection = new Option(LOAD_DEPENDENT_REDIRECTION, "[EXP] Local name servers start load balancing among name servers");
    Option nsLoadMonitorIntervalSeconds = new Option(LOAD_MONITOR_INTERVAL, true, "[EXP] Interval of monitoring load at every nameserver (seconds)");

    Option maxQueryWaitTime = new Option(MAX_QUERY_WAIT_TIME, true, "Maximum  wait time before query is  declared failed (milli-seconds)");
    Option queryTimeout = new Option(QUERY_TIMEOUT, true, "Request timeout interval (milli-seconds) before retransmission");
    Option adaptiveTimeout = new Option(ADAPTIVE_TIMEOUT, "[EXP] Whether to use an adaptive timeout instead a fixed timeout");
    Option delta = new Option(DELTA, true, "[EXP] Adaptive Retransmission: Weight assigned to latest sample in calculating moving average.");
    Option mu = new Option(MU, true, "[EXP] Adaptive Retransmission: Co-efficient of estimated RTT in calculating timeout.");
    Option phi = new Option(PHI, true, "[EXP] Adaptive Retransmission: Co-efficient of deviation in calculating timeout.");

    Option fileLoggingLevel = new Option(FILE_LOGGING_LEVEL, true, "Verbosity level of log file. Should be one of SEVERE, WARNING, INFO, CONFIG, FINE, FINER, FINEST.");
    Option consoleOutputLevel = new Option(CONSOLE_OUTPUT_LEVEL, true, "Verbosity level of console output. Should be one of SEVERE, WARNING, INFO, CONFIG, FINE, FINER, FINEST.");
    Option statFileLoggingLevel = new Option(STAT_FILE_LOGGING_LEVEL, true, "Verbosity level of log file for experiment related statistics. Should be one of SEVERE, WARNING, INFO, CONFIG, FINE, FINER, FINEST.");
    Option statConsoleOutputLevel = new Option(STAT_CONSOLE_OUTPUT_LEVEL, true, "Verbosity level of console output for experiment related statistics. Should be one of SEVERE, WARNING, INFO, CONFIG, FINE, FINER, FINEST.");

    Option emulatePingLatencies = new Option(EMULATE_PING_LATENCIES, "[EXP] Emulate a packet delay equal to ping delay in between two servers");
    Option variation = new Option(VARIATION, true, "[EXP] During emulation, what fraction of random variation to add to delay");

    Option address = OptionBuilder.withArgName("address").hasArg()
            .withDescription("Address")
            .create(ADDRESS);
    Option port = OptionBuilder.withArgName("port").hasArg()
            .withDescription("Port")
            .create(PORT);

    Option nsFile = OptionBuilder.withArgName("file").hasArg()
            .withDescription("File with node configuration of all name servers")
            .create(NS_FILE);

    Option regularWorkload = OptionBuilder.withArgName("size").hasArg()
            .withDescription("[EXP] Regular workload size")
            .create(REGULAR_WORKLOAD);
    Option mobileWorkload = OptionBuilder.withArgName("size").hasArg()
            .withDescription("[EXP] Mobile workload size")
            .create(MOBILE_WORKLOAD);

    Option workloadFile = OptionBuilder.withArgName("file").hasArg()
            .withDescription("[EXP] List of names that are queried by the local name server")
            .create(WORKLOAD_FILE);

    Option updateTraceFile = OptionBuilder.withArgName("file").hasArg()
            .withDescription("[EXP] Update Trace")
            .create(UPDATE_TRACE);

    Option outputSampleRate = new Option(OUTPUT_SAMPLE_RATE, true,
            "[EXP] Fraction of requests whose response time will be sampled");

    Option primaryReplicas = OptionBuilder.withArgName("#primaries").hasArg()
            .withDescription("Number of replica controllers for a name")
            .create(PRIMARY);

    Option alpha = OptionBuilder.withArgName("#").hasArg()
            .withDescription("[EXP] Beehive replication. Value of alpha in the Zipf distribution")
            .create(ALPHA);

    Option numLookups = OptionBuilder.withArgName("#").hasArg()
            .withDescription("[EXP] Number of lookups")
            .create(NUM_QUERY);
    Option numUpdates = OptionBuilder.withArgName("#").hasArg()
            .withDescription("[EXP] Number of Updates")
            .create(NUM_UPDATE);

    Option name = OptionBuilder.withArgName("name").hasArg()
            .withDescription("[EXP] Name of host/domain/device queried")
            .create(NAME);

    Option cacheSize = OptionBuilder.withArgName("#").hasArg()
            .withDescription("Size of cache at the local name server"
                    + ". Default = " + StartLocalNameServer.cacheSize)
            .create(CACHE_SIZE);

    Option voteInterval = OptionBuilder.withArgName("seconds").hasArg()
            .withDescription("Interval between votes sent by a local name server to replica controllers")
            .create(VOTE_INTERVAL);

    Option updateRateRegular = OptionBuilder.withArgName("ms").hasArg()
            .withDescription("[EXP] Inter-arrival time between updates for regular names")
            .create(UPDATE_RATE_REGULAR);

    commandLineOptions = new Options();
    commandLineOptions.addOption(configFile);
    commandLineOptions.addOption(address);
    commandLineOptions.addOption(port);
    commandLineOptions.addOption(nsFile);
    commandLineOptions.addOption(dnsGnsOnly);
    commandLineOptions.addOption(regularWorkload);
    commandLineOptions.addOption(mobileWorkload);
    commandLineOptions.addOption(workloadFile);
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
    commandLineOptions.addOption(updateRateRegular);
    commandLineOptions.addOption(debugMode);
    commandLineOptions.addOption(experimentMode);
    commandLineOptions.addOption(help);
    commandLineOptions.addOptionGroup(replication);
    commandLineOptions.addOption(beehiveDHTbase);
    commandLineOptions.addOption(beehiveLeafset);
    commandLineOptions.addOption(loadDependentRedirection);
    commandLineOptions.addOption(nsLoadMonitorIntervalSeconds);
    commandLineOptions.addOption(maxQueryWaitTime);
    commandLineOptions.addOption(queryTimeout);
    commandLineOptions.addOption(adaptiveTimeout);
    commandLineOptions.addOption(delta);
    commandLineOptions.addOption(mu);
    commandLineOptions.addOption(phi);
    commandLineOptions.addOption(fileLoggingLevel);
    commandLineOptions.addOption(consoleOutputLevel);
    commandLineOptions.addOption(statConsoleOutputLevel);
    commandLineOptions.addOption(statFileLoggingLevel);
    commandLineOptions.addOption(emulatePingLatencies);
    commandLineOptions.addOption(variation);
  }

  private static HelpFormatter formatter = new HelpFormatter();

  @SuppressWarnings("static-access")
  /**
   * ************************************************************
   * Initialized a command line parser ***********************************************************
   */
  private static CommandLine initializeOptions(String[] args) throws ParseException {
    CommandLineParser parser = new GnuParser();

    return parser.parse(commandLineOptions, args);
  }

  /**
   * ************************************************************
   * Prints command line usage ***********************************************************
   */
  private static void printUsage() {
    formatter.setWidth(135);
    formatter.printHelp(StartLocalNameServer.class.getCanonicalName(), HELP_HEADER, commandLineOptions, HELP_FOOTER);
  }

  /*
   * Sample invocation
   * 
   java -Xmx2g -cp ../../build/jars/GNS.jar edu.umass.cs.gns.main.StartLocalNameServer -address 127.0.0.1 -port 24398
   -nsfile name-server-info 
   -cacheSize 10000 -primary 3 -location -vInterval 1000 -chooseFromClosestK 1 -lookupRate 10000 
   -updateRateMobile 0 -updateRateRegular 10000 -maxQueryWaitTime 100000 -queryTimeout 100
   -fileLoggingLevel FINE -consoleOutputLevel INFO -statFileLoggingLevel INFO -statConsoleOutputLevel INFO
   -debuggingEnabled
   */
  /**
   * ************************************************************
   * Main method that starts the local name server with the given command line options.
   *
   * @param args Command line arguments
   *  ***********************************************************
   */
  public static void main(String[] args) {
    // all parameters will be specified in the arg list
    startLNS(null, -1, null, args);
  }

  // supports old style single name-server-info style as nsFile as well as new format 
  public static void startLNS(String address, int port, String nsFile, String... args) {
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
      if (parser.hasOption(CONFIG_FILE)) {
        configFile = parser.getOptionValue(CONFIG_FILE);
      }

      startLNSConfigFile(address, port, nsFile, configFile, parser);
    } catch (Exception e1) {
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
   * @param id ID of local name server (GOING AWAY)
   * @param address // replaces id
   * @param nsFile node config file (can be null)
   * @param configFile config file with parameters (can be null)
   * @param parser command line arguments (can be null)
   */
  public static void startLNSConfigFile(String address, int port, String nsFile, String configFile, CommandLine parser) {
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
        for (String propertyName : prop.stringPropertyNames()) {
          allValues.put(propertyName, prop.getProperty(propertyName));
        }

      }

      // add options given via command line to hashmap. these options can override options given in config file.
      if (parser != null) {
        for (Option option : parser.getOptions()) {
          String argName = option.getOpt();
          String value = option.getValue();
          // if an option has a boolean value, the command line arguments do not say true/false for some of these options
          // if option name is given as argument on the command line, it means the value is true. therefore, the hashmap
          // will also assign the value true for these options.
          if (value == null) {
            value = "true";
          }
          allValues.put(argName, value);
        }
      }

      if (allValues.containsKey(ADDRESS)) {
        address = allValues.get(ADDRESS);
      }

      if (allValues.containsKey(PORT)) {
        port = Integer.parseInt(allValues.get(PORT));
      }

      if (allValues.containsKey(NS_FILE)) {
        nsFile = allValues.get(NS_FILE);
      }

      cacheSize = (allValues.containsKey(CACHE_SIZE)) ? Integer.parseInt(allValues.get(CACHE_SIZE)) : 10000;

      GNS.numPrimaryReplicas = allValues.containsKey(PRIMARY)
              ? Integer.parseInt(allValues.get(PRIMARY)) : GNS.DEFAULT_NUM_PRIMARY_REPLICAS;

      if (allValues.containsKey(LOCATION) && Boolean.parseBoolean(allValues.get(LOCATION))) {
        replicationFramework = ReplicationFrameworkType.LOCATION;
        voteIntervalMillis = Integer.parseInt(allValues.get(VOTE_INTERVAL)) * 1000;
      } else if (allValues.containsKey(BEEHIVE) && Boolean.parseBoolean(allValues.get(BEEHIVE))) {
        replicationFramework = ReplicationFrameworkType.BEEHIVE;
        BeehiveDHTRouting.beehive_DHTbase = Integer.parseInt(allValues.get(BEEHIVE_BASE));
        BeehiveDHTRouting.beehive_DHTleafsetsize = Integer.parseInt(allValues.get(LEAF_SET));
      } else {
        replicationFramework = GNS.DEFAULT_REPLICATION_FRAMEWORK;
      }

      loadDependentRedirection = allValues.containsKey(LOAD_DEPENDENT_REDIRECTION)
              && Boolean.parseBoolean(allValues.get(LOAD_DEPENDENT_REDIRECTION));
      nameServerLoadMonitorIntervalSeconds = (loadDependentRedirection)
              ? Integer.parseInt(allValues.get(LOAD_MONITOR_INTERVAL)) : 60;

      maxQueryWaitTime = (allValues.containsKey(MAX_QUERY_WAIT_TIME))
              ? Integer.parseInt(allValues.get(MAX_QUERY_WAIT_TIME)) : GNS.DEFAULT_MAX_QUERY_WAIT_TIME;

      queryTimeout = (allValues.containsKey(QUERY_TIMEOUT))
              ? Integer.parseInt(allValues.get(QUERY_TIMEOUT)) : GNS.DEFAULT_QUERY_TIMEOUT;

      adaptiveTimeout = allValues.containsKey(ADAPTIVE_TIMEOUT) && Boolean.parseBoolean(allValues.get(ADAPTIVE_TIMEOUT));
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
      updateTraceFile = allValues.containsKey(UPDATE_TRACE) ? allValues.get(UPDATE_TRACE) : null;
      workloadFile = allValues.containsKey(WORKLOAD_FILE) ? allValues.get(WORKLOAD_FILE) : null;

      // all parameters related to synthetic workload
      isSyntheticWorkload = allValues.containsKey(ZIPF) && Boolean.parseBoolean(allValues.get(ZIPF));
      alpha = (isSyntheticWorkload) ? Double.parseDouble(allValues.get(ALPHA)) : 0;
      regularWorkloadSize = (isSyntheticWorkload) ? Integer.parseInt(allValues.get(REGULAR_WORKLOAD)) : 0;
      mobileWorkloadSize = (isSyntheticWorkload) ? Integer.parseInt(allValues.get(MOBILE_WORKLOAD)) : 0;

      name = (isSyntheticWorkload) ? allValues.get("name") : null;
      // made these optional for non-experimental use
      numQuery = allValues.containsKey(NUM_QUERY) ? Integer.parseInt(allValues.get(NUM_QUERY)) : 0;
      numUpdate = allValues.containsKey(NUM_UPDATE) ? Integer.parseInt(allValues.get(NUM_UPDATE)) : 0;

      outputSampleRate = allValues.containsKey(OUTPUT_SAMPLE_RATE)
              ? Double.parseDouble(allValues.get(OUTPUT_SAMPLE_RATE)) : 1.0;

      if (allValues.containsKey(DEBUG_MODE)) {
        debuggingEnabled = Boolean.parseBoolean(allValues.get(DEBUG_MODE));
      }

      if (allValues.containsKey(DNS_GNS_ONLY)) {
        dnsGnsOnly = Boolean.parseBoolean(allValues.get(DNS_GNS_ONLY));
      }
      if (allValues.containsKey(EXPERIMENT_MODE)) {
        experimentMode = Boolean.parseBoolean(allValues.get(EXPERIMENT_MODE));
      }

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

      emulatePingLatencies = allValues.containsKey(EMULATE_PING_LATENCIES)
              && Boolean.parseBoolean(allValues.get(EMULATE_PING_LATENCIES));

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
    GNS.getLogger().info("Date: " + dateFormat.format(date));
    GNS.getLogger().info("Address: " + address);
    GNS.getLogger().info("Port: " + port);
    GNS.getLogger().info("NS File: " + nsFile);
    GNS.getLogger().info("Regular Workload Size: " + regularWorkloadSize);
    GNS.getLogger().info("Mobile Workload Size: " + mobileWorkloadSize);
    GNS.getLogger().info("Workload File: " + workloadFile);
    GNS.getLogger().info("Update Trace File: " + updateTraceFile);
    GNS.getLogger().info("Primary: " + GNS.numPrimaryReplicas);
    GNS.getLogger().info("Alpha: " + alpha);
    GNS.getLogger().info("Lookups: " + numQuery);
    GNS.getLogger().info("Updates: " + numUpdate);
    GNS.getLogger().info("Zipf Workload: " + isSyntheticWorkload);
    GNS.getLogger().info("Replication: " + replicationFramework.toString());
    GNS.getLogger().info("Vote Interval: " + voteIntervalMillis + "ms");
    GNS.getLogger().info("Cache Size: " + cacheSize);
    GNS.getLogger().info("Experiment Mode: " + experimentMode);
    GNS.getLogger().info("DNS GNS Only: " + dnsGnsOnly);
    GNS.getLogger().info("Debug Mode: " + debuggingEnabled);

    try {
      GNSNodeConfig gnsNodeConfig;
      gnsNodeConfig = new GNSNodeConfig(nsFile, GNSNodeConfig.BOGUS_NULL_NAME_SERVER_ID);

      //Start local name server
      new LocalNameServer(new InetSocketAddress(address, port), gnsNodeConfig);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
