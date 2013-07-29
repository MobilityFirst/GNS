package edu.umass.cs.gns.main;

//import edu.umass.cs.gnrs.nameserver.NSListenerUpdate;
import edu.umass.cs.gns.nameserver.GenerateSyntheticRecordTable;
import edu.umass.cs.gns.nameserver.NameServer;
import edu.umass.cs.gns.util.ConfigFileInfo;
import edu.umass.cs.gns.util.HashFunction;
import org.apache.commons.cli.*;
import edu.umass.cs.gns.paxos.FailureDetection;
import edu.umass.cs.gns.paxos.PaxosLogger;
import edu.umass.cs.gns.paxos.PaxosLogger2;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import static edu.umass.cs.gns.util.Util.println;

/**
 * ************************************************************
 * Starts a single instance of the Nameserver with the specified parameters.
 *
 * @author Hardeep Uppal ***********************************************************
 */
public class StartNameServer {

  public static int regularWorkloadSize;
  public static int mobileWorkloadSize;
  public static boolean staticReplication = false;
  public static boolean randomReplication = false;
  public static boolean locationBasedReplication = false;
  public static boolean beehiveReplication = false;
  public static double C;
  public static double base;
  public static double alpha;
  public static long aggregateInterval;
  public static long analysisInterval;
  public static double normalizingConstant;
  public static int movingAverageWindowSize;
  public static double ttlConstant;
  public static int defaultTTLRegularName;
  public static int defaultTTLMobileName;
  public static int nameServerVoteSize;
  //public static boolean isPlanetlab = false;
  public static boolean debugMode = true;
  public static boolean persistentDataStore = false;
  public static boolean simpleDiskStore = true;
  // incore with disk backup
  public static String dataFolder = "/state/partition1/";
  // is in experiment mode: abhigyan.
  // use this flag to make changes to code which will only run during experiments.
  public static boolean experimentMode = false;
  public static int workerThreadCount = 5; // number of worker threads
  public static boolean tinyUpdate = false; // 
  private static HelpFormatter formatter = new HelpFormatter();
  private static Options commandLineOptions;
  public static boolean optimalReplication = false;
  public static boolean kmediodsReplication = false;
  public static int numberLNS;
  public static String lnsnsPingFile;
  public static String nsnsPingFile;
  public static int minReplica = 3;
  public static int maxReplica = 100;
  public static String specifiedActives = "specifiedActive.txt";
  public static int loadMonitorWindow = 100;

  @SuppressWarnings("static-access")
  /**
   * ************************************************************
   * Initialized a command line parser ***********************************************************
   */
  private static CommandLine initializeOptions(String[] args) throws ParseException {
    Option help = new Option("help", "Prints Usage");
    Option local = new Option("local", "Run NameServer instance on localhost");
//    Option planetlab = new Option("planetlab", "Run NameServer instance on a PlanetLab Machine");
//    OptionGroup environment = new OptionGroup().addOption(local).addOption(planetlab);

    Option staticReplication = new Option("static", "Fixed number of active nameservers per name");
    Option randomReplication = new Option("random", "Randomly select new active nameservers");
    Option locationBasedReplication = new Option("location", "Location Based selection of active nameserervs");
    Option beehiveReplication = new Option("beehive", "Beehive Replication");
    Option kmediodsReplication = new Option("kmediods", "K-Mediods Replication");
    Option optimalReplication = new Option("optimal", "Optimal Replication");
    OptionGroup replication = new OptionGroup().addOption(staticReplication)
            .addOption(randomReplication)
            .addOption(locationBasedReplication)
            .addOption(beehiveReplication)
            .addOption(kmediodsReplication)
            .addOption(optimalReplication);


    Option workerThreadCount = new Option("workerThreadCount", true, "Number of worker threads");
    Option tinyUpdate = new Option("tinyUpdate", "Use a smaller update packet");


    Option debugMode = new Option("debugMode", "Run in debug mode");
    Option experimentMode = new Option("experimentMode", "Run in experiment mode");
    Option persistentDataStore = new Option("persistentDataStore", "Use a persistent data store for name records");
    Option simpleDiskStore = new Option("simpleDiskStore", "Use a simple disk store for name records");
    Option dataFolder = new Option("dataFolder", true, "dataFolder");


    Option fileLoggingLevel = new Option("fileLoggingLevel", true, "fileLoggingLevel");
    Option consoleOutputLevel = new Option("consoleOutputLevel", true, "consoleOutputLevel");
    Option statFileLoggingLevel = new Option("statFileLoggingLevel", true, "statFileLoggingLevel");
    Option statConsoleOutputLevel = new Option("statConsoleOutputLevel", true, "statConsoleOutputLevel");

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
    Option syntheticWorkloadSleepTimeBetweenAddingNames =
            new Option("syntheticWorkloadSleepTimeBetweenAddingNames", true, "syntheticWorkloadSleepTimeBetweenAddingNames");

    Option primaryReplicas = OptionBuilder.withArgName("#primaries").hasArg()
            .withDescription("Number of primary nameservers")
            .create("primary");

    Option aggregateInterval = OptionBuilder.withArgName("seconds").hasArg()
            .withDescription("Interval between collecting stats")
            .create("aInterval");

    Option replicationInterval = OptionBuilder.withArgName("seconds").hasArg()
            .withDescription("Interval between replication")
            .create("rInterval");

    Option normalizingConstant = OptionBuilder.withArgName("#").hasArg()
            .withDescription("Normalizing constant")
            .create("nconstant");

//    Option primaryPaxos = new Option("primaryPaxos","Whether primaries use paxos between themselves");
//    Option paxosDiskBackup = new Option("paxosDiskBackup", "Whether paxos stores its state to disk");
    Option paxosLogFolder = new Option("paxosLogFolder", true, "Folder where paxos logs are stored in a persistent manner.");
    Option failureDetectionMsgInterval = new Option("failureDetectionMsgInterval", true,
            "Interval (in sec) between two failure detection messages sent to a node");
    Option failureDetectionTimeoutInterval = new Option("failureDetectionTimeoutInterval", true,
            "Interval (in sec) after which a node is declared as failed");

    Option movingAverageWindowSize = OptionBuilder.withArgName("size").hasArg()
            .withDescription("Size of window to calculat the "
            + "moving average of update inter-arrival time")
            .create("mavg");

    Option ttlConstant = OptionBuilder.withArgName("#").hasArg()
            .withDescription("TTL constant")
            .create("ttlconstant");
    Option defaultTTLRegularNames = OptionBuilder.withArgName("#seconds").hasArg()
            .withDescription("Default TTL value for regular names")
            .create("rttl");
    Option defaultTTLMobileNames = OptionBuilder.withArgName("#seconds").hasArg()
            .withDescription("Default TTL value for mobile names")
            .create("mttl");
    Option nameServerVoteSize = OptionBuilder.withArgName("size").hasArg()
            .withDescription("Size of name server selection vote")
            .create("nsVoteSize");

    Option minReplica = new Option("minReplica", true, "Minimum number of replica");
    Option maxReplica = new Option("maxReplica", true, "Maximum number of replica");

    Option C = OptionBuilder.withArgName("hop").hasArg()
            .withDescription("Average hop count")
            .create("C");
    Option base = OptionBuilder.withArgName("#").hasArg()
            .withDescription("Base of DHT")
            .create("base");
    Option alpha = OptionBuilder.withArgName("#").hasArg()
            .withDescription("Zipf distribution")
            .create("alpha");

    Option numberLNS = OptionBuilder.withArgName("size").hasArg()
            .withDescription("Number of LNS")
            .create("numLNS");
    Option nsnsPingFile = OptionBuilder.withArgName("file").hasArg()
            .withDescription("File with all NS-NS ping latencies")
            .create("nsnsping");
    Option lnsnsPingFile = OptionBuilder.withArgName("file").hasArg()
            .withDescription("File with all LNS-NS ping latencies")
            .create("lnsnsping");
    Option signatureCheck = new Option("signatureCheck",
            "whether an update operation checks signature or not");

    commandLineOptions = new Options();
    commandLineOptions.addOption(nodeId);
    commandLineOptions.addOption(nsFile);
    commandLineOptions.addOption(local);
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
    commandLineOptions.addOption(persistentDataStore);
    commandLineOptions.addOption(simpleDiskStore);
    commandLineOptions.addOption(dataFolder);

    commandLineOptions.addOption(paxosLogFolder);
    commandLineOptions.addOption(failureDetectionMsgInterval);
    commandLineOptions.addOption(failureDetectionTimeoutInterval);

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

    CommandLineParser parser = new GnuParser();
    return parser.parse(commandLineOptions, args);
  }

  /**
   * ************************************************************
   * Prints command line usage ***********************************************************
   */
  private static void printUsage() {
    formatter.printHelp("StartNameServer", commandLineOptions);
  }
  /**
   * ************************************************************
   * Main method that starts the local name server with the given command line options.
   *
   * @param args Command line arguments
   * @throws ParseException ***********************************************************
   */
  private static final String DEFAULTAGGREGATEINTERVAL = "600"; // seconds
  private static final String DEFAULTANALYSISINTERVAL = "600";  // seconds
  private static final String DEFAULTNORMALIZINGCONSTANT = "1";
  private static final String DEFAULTMOVINGAVERAGEWINDOWSIZE = "20";
  private static final String DEFAULTTTLCONSTANT = "0.0000001";
  private static final String DEFAULTTTLREGULARNAME = "0";
  private static final String DEFAULTTTLMOBILENAME = "0";
  private static final String DEFAULTREGULARWORKLOADSIZE = "0";
  private static final String DEFAULTMOBILEMOBILEWORKLOADSIZE = "0";
  private static final String DEFAULTPAXOSLOGPATHNAME = "log/paxos_log";

  public static void main(String[] args) {
    int id = 0;					//node id
    boolean isLocal = false;	//Flag indicating whether this instance is running all servers on local host
    String nsFile = "";			//Nameserver file

    try {
      CommandLine parser = initializeOptions(args);
      if (parser.hasOption("help")) {
        printUsage();
        System.exit(1);
      }
      isLocal = parser.hasOption("local");
      id = Integer.parseInt(parser.getOptionValue("id"));
      nsFile = parser.getOptionValue("nsfile");
      GNS.numPrimaryReplicas = Integer.parseInt(parser.getOptionValue("primary", Integer.toString(GNS.DEFAULTNUMPRIMARYREPLICAS)));
      aggregateInterval = Integer.parseInt(parser.getOptionValue("aInterval", DEFAULTAGGREGATEINTERVAL)) * 1000;
      analysisInterval = Integer.parseInt(parser.getOptionValue("rInterval", DEFAULTANALYSISINTERVAL)) * 1000;
      normalizingConstant = Double.parseDouble(parser.getOptionValue("nconstant", DEFAULTNORMALIZINGCONSTANT));
      movingAverageWindowSize = Integer.parseInt(parser.getOptionValue("mavg", DEFAULTMOVINGAVERAGEWINDOWSIZE));

      ttlConstant = Double.parseDouble(parser.getOptionValue("ttlconstant", DEFAULTTTLCONSTANT));
      defaultTTLRegularName = Integer.parseInt(parser.getOptionValue("rttl", DEFAULTTTLREGULARNAME));
      defaultTTLMobileName = Integer.parseInt(parser.getOptionValue("mttl", DEFAULTTTLMOBILENAME));

      regularWorkloadSize = Integer.parseInt(parser.getOptionValue("rworkload", DEFAULTREGULARWORKLOADSIZE));
      mobileWorkloadSize = Integer.parseInt(parser.getOptionValue("mworkload", DEFAULTMOBILEMOBILEWORKLOADSIZE));

      GenerateSyntheticRecordTable.sleepBetweenNames = Integer.parseInt(parser.getOptionValue("syntheticWorkloadSleepTimeBetweenAddingNames", "0"));

      staticReplication = parser.hasOption("static");
      randomReplication = parser.hasOption("random");

      locationBasedReplication = parser.hasOption("location");
      nameServerVoteSize = (locationBasedReplication)
              ? Integer.parseInt(parser.getOptionValue("nsVoteSize")) : 0;
      if (parser.hasOption("minReplica")) {
        minReplica = Integer.parseInt(parser.getOptionValue("minReplica"));
      }
      if (parser.hasOption("maxReplica")) {
        maxReplica = Integer.parseInt(parser.getOptionValue("maxReplica"));
      }

      beehiveReplication = parser.hasOption("beehive");
      C = (beehiveReplication) ? Double.parseDouble(parser.getOptionValue("C")) : 0;
      base = (beehiveReplication) ? Double.parseDouble(parser.getOptionValue("base")) : 0;
      alpha = (beehiveReplication) ? Double.parseDouble(parser.getOptionValue("alpha")) : 0;

      optimalReplication = parser.hasOption("optimal");

      debugMode = parser.hasOption("debugMode");
      experimentMode = parser.hasOption("experimentMode");
      persistentDataStore = parser.hasOption("persistentDataStore");
      simpleDiskStore = parser.hasOption("simpleDiskStore");


      if (simpleDiskStore && parser.hasOption("dataFolder")) {
        dataFolder = parser.getOptionValue("dataFolder");
      }


//      primaryPaxos = parser.hasOption("primaryPaxos");
//      PaxosManager.writeStateToDisk = parser.hasOption("paxosDiskBackup");
//      String paxosLogFolder ;
//      if (PaxosManager.writeStateToDisk && parser.hasOption("paxosLogFolder")) {
//      }
      if (parser.hasOption("paxosLogFolder") == false) {
        PaxosLogger2.logFolder = DEFAULTPAXOSLOGPATHNAME;
      } else {
        PaxosLogger2.logFolder = parser.getOptionValue("paxosLogFolder");
      }

      if (parser.hasOption("failureDetectionMsgInterval")) {
        FailureDetection.pingInterval =
                Integer.parseInt(parser.getOptionValue("failureDetectionMsgInterval")) * 1000;
      }
      if (parser.hasOption("failureDetectionTimeoutInterval")) {
        FailureDetection.timeoutInterval =
                Integer.parseInt(parser.getOptionValue("failureDetectionTimeoutInterval")) * 1000;
      }


      if (parser.hasOption("workerThreadCount")) {
        workerThreadCount = Integer.parseInt(parser.getOptionValue("workerThreadCount"));
      }

      tinyUpdate = parser.hasOption("tinyUpdate");

      kmediodsReplication = parser.hasOption("kmediods");
      numberLNS = (kmediodsReplication) ? Integer.parseInt(parser.getOptionValue("numLNS")) : 0;
      lnsnsPingFile = (kmediodsReplication) ? parser.getOptionValue("lnsnsping") : null;
      nsnsPingFile = (kmediodsReplication) ? parser.getOptionValue("nsnsping") : null;

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

//      NSListenerUpdate.doSignatureCheck = parser.hasOption("signatureCheck");
    } catch (Exception e1) {
      printUsage();
      e1.printStackTrace();
      System.exit(1);
    }

    DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
    Date date = new Date();
    println("Date: " + dateFormat.format(date), debugMode);
    println("Id: " + id, debugMode);
    println("NS File: " + nsFile, debugMode);
    println("Local: " + isLocal, debugMode);
    println("Persistant Data Store: " + persistentDataStore, debugMode);
    println("Regular Workload Size: " + regularWorkloadSize, debugMode);
    println("Mobile Workload Size: " + mobileWorkloadSize, debugMode);
    println("Primary: " + GNS.numPrimaryReplicas, debugMode);
    println("Static Replication: " + staticReplication, debugMode);
    println("Random Replication: " + randomReplication, debugMode);
    println("Location Based Replication: " + locationBasedReplication, debugMode);
    println("Name Server Selection Vote Size: " + nameServerVoteSize, debugMode);
    println("Beehive Replication: " + beehiveReplication, debugMode);
    println("C: " + C, debugMode);
    println("DHT Base: " + base, debugMode);
    println("Alpha: " + alpha, debugMode);
    println("Aggregate Interval: " + aggregateInterval + "ms", debugMode);
    println("Replication Interval: " + analysisInterval + "ms", debugMode);
    println("Normalizing Constant: " + normalizingConstant, debugMode);
    println("Moving Average Window: " + movingAverageWindowSize, debugMode);
    println("TTL Constant: " + ttlConstant, debugMode);
    println("Default TTL Regular Names: " + defaultTTLRegularName, debugMode);
    println("Default TTL Mobile Names: " + defaultTTLMobileName, debugMode);
    println("Debug Mode: " + debugMode, debugMode);
    println("Experiment Mode: " + experimentMode, debugMode);

    try {
      HashFunction.initializeHashFunction();
      //Generate name server lookup table
      ConfigFileInfo.readHostInfo(nsFile, id);
      //Start nameserver 
      new NameServer(id).run();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
