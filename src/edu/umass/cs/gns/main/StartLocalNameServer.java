package edu.umass.cs.gns.main;

import edu.umass.cs.gns.localnameserver.LocalNameServer;
import edu.umass.cs.gns.replicationframework.BeehiveDHTRouting;
import edu.umass.cs.gns.util.AdaptiveRetransmission;
import edu.umass.cs.gns.util.ConfigFileInfo;
import edu.umass.cs.gns.util.HashFunction;
import org.apache.commons.cli.*;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import static edu.umass.cs.gns.util.Util.println;


/**
 * ************************************************************
 * Starts a single instance of the Local Nameserver with the specified parameters.
 *
 * @author Hardeep Uppal ***********************************************************
 */
public class StartLocalNameServer {

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
  public static boolean locationBasedReplication = false;
  public static long voteInterval;
  
  public static boolean isSyntheticWorkload = false;
  public static String name;
  public static int cacheSize = 1000;
  public static boolean debugMode = false;
  private static HelpFormatter formatter = new HelpFormatter();
  private static Options commandLineOptions;
  public static double lookupRate;
  public static double updateRateMobile;
  public static double updateRateRegular;
  public static boolean optimalReplication = false;
  public static String optimalTrace = null;
  public static int replicationInterval = 0;

  public static double outputSampleRate = 1.0;
  /**
   * Variant of voting, in which votes are sent not for closest but for a random node among k-closest.
   * Name server chosen for a name is the (name modulo k)-th closest node.
   */
  public static int chooseFromClosestK = 1;
  
  /**
   * Used for running experiments for Auspice paper.
   */
  public static boolean experimentMode = false;
  /**
   * Whether beehive replication is used or not.
   */
  public static boolean beehiveReplication = false;

  /**
   * Whether beehive replication is used or not.
   */
  public static boolean replicateAll = false;

  //Abhigyan: parameters related to retransmissions.
  // More parameters in util.AdaptiveRetransmission.java
  /**
   * Maximum number of transmission of a query *
   */
  public static int numberOfTransmissions = GNS.DEFAULT_NUMBER_OF_TRANSMISSIONS;
  /**
   * Maxmimum time a local name server waits for a response from name server query is logged as failed after this.
   */
  public static int maxQueryWaitTime = GNS.DEFAULT_MAX_QUERY_WAIT_TIME; 
  /**
   * Fixed timeout after which a query retransmitted.
   */
  public static int queryTimeout = GNS.DEFAULT_QUERY_TIMEOUT;

//  public static int MAX_RESTARTS = 3;

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
  
  public static boolean delayScheduling = false;
  
  public static double variation = 0.1; // 10 % addition

  public static boolean runHttpServer = true;
  
  @SuppressWarnings("static-access")
  /**
   * ************************************************************
   * Initialized a command line parser ***********************************************************
   */
  private static CommandLine initializeOptions(String[] args) throws ParseException {
    Option help = new Option("help", "Prints Usage");
    Option local = new Option("local", "Run Local NameServer instance on localhost");
//    Option planetlab = new Option("planetlab", "Run Local NameServer instance on a PlanetLab Machine");
//    OptionGroup environment = new OptionGroup().addOption(local).addOption(planetlab);

    Option debugMode = new Option("debugMode", "Run in debug mode");
    Option experimentMode = new Option("experimentMode", "Mode to run experiments for Auspice paper");


    Option syntheticWorkload = new Option("zipf", "Use Zipf distribution to generate worklaod");
    Option locationBasedReplication = new Option("location", "Location Based selection of active nameserervs");
    Option optimalReplication = new Option("optimal", "Optimal replication");

    Option beehiveReplication = new Option("beehive", "Beehive replication");
    Option beehiveDHTbase = new Option("beehiveBase", true, "Beehive DHT base, default 16");
    Option beehiveLeafset = new Option("leafSet", true, "Beehive Leaf set size, must be less thant number of name servers, default 24");

    Option loadDependentRedirection = new Option("loadDependentRedirection", "local name servers start load balancing among name servers");
    Option nsLoadMonitorIntervalSeconds = new Option("nsLoadMonitorIntervalSeconds", true, "interval of monitoring load at every nameserver (seconds)");

    Option maxQueryWaitTime = new Option("maxQueryWaitTime", true, "maximum  Wait Time before query is  declared failed (milli-seconds)");
    Option numberOfTransmissions = new Option("numberOfTransmissions", true, "maximum number of times a query is transmitted.");
    Option queryTimeout = new Option("queryTimeout", true, "query timeout interval (milli-seconds)");
    Option adaptiveTimeout = new Option("adaptiveTimeout", "Whether to use an adaptive timeout or a fixed timeout");
    Option delta = new Option("delta", true, "Adaptive Retransmission: Weight assigned to latest sample in calculating moving average.");
    Option mu = new Option("mu", true, "Adaptive Retransmission: Co-efficient of estimated RTT in calculating timeout.");
    Option phi = new Option("phi", true, "Adaptive Retransmission: Co-efficient of deviation in calculating timeout.");
    
    Option  chooseFromClosestK = new Option("chooseFromClosestK", true, "chooseFromClosestK");
    
    Option fileLoggingLevel = new Option("fileLoggingLevel", true, "fileLoggingLevel");
    Option consoleOutputLevel = new Option("consoleOutputLevel", true, "consoleOutputLevel");
    Option statFileLoggingLevel = new Option("statFileLoggingLevel", true, "statFileLoggingLevel");
    Option statConsoleOutputLevel = new Option("statConsoleOutputLevel", true, "statConsoleOutputLevel");

    Option tinyQuery = new Option("tinyQuery", "tiny query mode");
    Option delayScheduling = new Option("delayScheduling",  "add packet delay equal to ping delay between two servers (used for emulation).");
    Option variation = new Option("variation", true,"variation");

    Option nodeId = OptionBuilder.withArgName("nodeId").hasArg()
            .withDescription("Node id")
            .create("id");

    Option nsFile = OptionBuilder.withArgName("file").hasArg()
            .withDescription("Name server file")
            .create("nsfile");

    Option regularWorkload = OptionBuilder.withArgName("size").hasArg()
            .withDescription("Regular workload size")
            .create("rworkload");
    Option mobileWorkload = OptionBuilder.withArgName("size").hasArg()
            .withDescription("Mobile workload size")
            .create("mworkload");

    Option workloadFile = OptionBuilder.withArgName("file").hasArg()
            .withDescription("List of names that are queried by the local name server")
            .create("wfile");

    Option lookupTraceFile = OptionBuilder.withArgName("file").hasArg()
            .withDescription("Lookup Trace")
            .create("lookupTrace");

    Option updateTraceFile = OptionBuilder.withArgName("file").hasArg()
            .withDescription("Update Trace")
            .create("updateTrace");
    Option outputSampleRate = new Option("outputSampleRate",true, "fraction of requests whose response time will be sampled");

    Option primaryReplicas = OptionBuilder.withArgName("#primaries").hasArg()
            .withDescription("Number of primary nameservers")
            .create("primary");

    Option alpha = OptionBuilder.withArgName("#").hasArg()
            .withDescription("Value of alpha in the Zipf Distribution")
            .create("alpha");

    Option numLookups = OptionBuilder.withArgName("#").hasArg()
            .withDescription("Number of lookups")
            .create("nlookup");
    Option numUpdates = OptionBuilder.withArgName("#").hasArg()
            .withDescription("Number of Updates")
            .create("nUpdate");

    Option name = OptionBuilder.withArgName("name").hasArg()
            .withDescription("Name of host/domain/device queried")
            .create("name");

    Option cacheSize = OptionBuilder.withArgName("#").hasArg()
            .withDescription("Size of cache at the local name server"
            + ".Default 1000")
            .create("cacheSize");

    Option voteInterval = OptionBuilder.withArgName("seconds").hasArg()
            .withDescription("Interval between nameserver votes")
            .create("vInterval");

    Option lookupRate = OptionBuilder.withArgName("ms").hasArg()
            .withDescription("Inter-arrival time between lookups")
            .create("lookupRate");

    Option updateRateMobile = OptionBuilder.withArgName("ms").hasArg()
            .withDescription("Inter-arrival time between updates for mobile names")
            .create("updateRateMobile");

    Option updateRateRegular = OptionBuilder.withArgName("ms").hasArg()
            .withDescription("Inter-arrival time between updates for regular names")
            .create("updateRateRegular");

    Option replicationInverval = OptionBuilder.withArgName("seconds").hasArg()
            .withDescription("Interval between replication")
            .create("rInterval");
    Option optimalTrace = OptionBuilder.withArgName("file").hasArg()
            .withDescription("Optimal trace file")
            .create("optimalTrace");



    Option runHttpServer = new Option("runHttpServer", "run the http server in the same process as local name server.");

    commandLineOptions = new Options();
    commandLineOptions.addOption(nodeId);
    commandLineOptions.addOption(nsFile);
    commandLineOptions.addOption(local);
    //commandLineOptions.addOptionGroup(environment);
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
    commandLineOptions.addOption(locationBasedReplication);
    commandLineOptions.addOption(voteInterval);
    commandLineOptions.addOption(chooseFromClosestK);
    commandLineOptions.addOption(cacheSize);
    commandLineOptions.addOption(lookupRate);
    commandLineOptions.addOption(updateRateMobile);
    commandLineOptions.addOption(updateRateRegular);
    commandLineOptions.addOption(optimalReplication);
    commandLineOptions.addOption(replicationInverval);
    commandLineOptions.addOption(optimalTrace);
    commandLineOptions.addOption(debugMode);
    commandLineOptions.addOption(experimentMode);
    commandLineOptions.addOption(help);

    commandLineOptions.addOption(beehiveReplication);
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
    boolean isLocal = false;	// flag indicating run all servers on local host
    String nsFile = "";				//Nameserver file

    try {
      CommandLine parser = initializeOptions(args);
      if (parser.hasOption("help")) {
        printUsage();
        System.exit(1);
      }
      isLocal = parser.hasOption("local");
      //isPlanetLab = parser.hasOption("planetlab");
      id = Integer.parseInt(parser.getOptionValue("id"));
      nsFile = parser.getOptionValue("nsfile");
      cacheSize = (parser.hasOption("cacheSize"))
              ? Integer.parseInt(parser.getOptionValue("cacheSize")) : 1000;
      GNS.numPrimaryReplicas = Integer.parseInt(parser.getOptionValue("primary", Integer.toString(GNS.DEFAULTNUMPRIMARYREPLICAS)));

      beehiveReplication = parser.hasOption("beehive");
      BeehiveDHTRouting.beehive_DHTbase = (beehiveReplication) ? Integer.parseInt(parser.getOptionValue("beehiveBase")) : 16;
      BeehiveDHTRouting.beehive_DHTleafsetsize = (beehiveReplication) ? Integer.parseInt(parser.getOptionValue("leafSet")) : 24;

      
      loadDependentRedirection = parser.hasOption("loadDependentRedirection");
      nameServerLoadMonitorIntervalSeconds = (loadDependentRedirection) ? Integer.parseInt(parser.getOptionValue("nsLoadMonitorIntervalSeconds")) : 60;

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

      locationBasedReplication = parser.hasOption("location");

      // parameters for optimal.
      optimalReplication = parser.hasOption("optimal");
      optimalTrace = (optimalReplication) ? parser.getOptionValue("optimalTrace") : null;
      replicationInterval = (optimalReplication)
              ? Integer.parseInt(parser.getOptionValue("rInterval")) * 1000 : 0;

      // vote interval in ms
      voteInterval = (locationBasedReplication)
              ? Integer.parseInt(parser.getOptionValue("vInterval")) * 1000 : 0;
      // 
      if (locationBasedReplication && parser.hasOption("chooseFromClosestK")) {
      	chooseFromClosestK = Integer.parseInt(parser.getOptionValue("chooseFromClosestK"));
      }
      
      name = parser.getOptionValue("name");
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
      
      if (parser.hasOption("delayScheduling")) {
	      delayScheduling = parser.hasOption("delayScheduling");
	      if(delayScheduling && parser.hasOption("variation")) 
	      	variation = Double.parseDouble(parser.getOptionValue("variation"));
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
    println("Local: " + isLocal, debugMode);
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
    println("Name: " + name, debugMode);
    println("Location Based Replication: " + locationBasedReplication, debugMode);
    println("Vote Interval: " + voteInterval + "ms", debugMode);
    println("Cache Size: " + cacheSize, debugMode);
    println("Experiment Mode: " + experimentMode, debugMode);
    println("Debug Mode: " + debugMode, debugMode);

    try {
      HashFunction.initializeHashFunction();
      ConfigFileInfo.readHostInfo(nsFile, id);
      //Start local name server 
      new LocalNameServer(id).run();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
