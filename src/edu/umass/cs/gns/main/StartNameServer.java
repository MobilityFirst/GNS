package edu.umass.cs.gns.main;

//import edu.umass.cs.gnrs.nameserver.NSListenerUpdate;
import edu.umass.cs.gns.nameserver.NameServer;
import edu.umass.cs.gns.paxos.PaxosManager;
import edu.umass.cs.gns.test.FailureScenario;
import edu.umass.cs.gns.util.ConfigFileInfo;
import edu.umass.cs.gns.util.HashFunction;
import org.apache.commons.cli.*;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import static edu.umass.cs.gns.util.Util.println;

/**
 * 
 * Starts a single instance of the Nameserver with the specified parameters.
 *
 */
public class StartNameServer {

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
  public static int defaultTTLRegularName;
  public static int defaultTTLMobileName;
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
  public static boolean eventualConsistency = false;

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
  public static String nameActives;
  public static int loadMonitorWindow = 5000;

  // in experiment mode, the default paxos coordinator will send prepare message at a random time between
  // paxosStartMinDelaySec  and paxosStartMaxDelaySec
  public static int paxosStartMinDelaySec = 0;
  public static int paxosStartMaxDelaySec = 0;


  public static int quitNodeID = -1; // only for testing.
  public static int quitAfterTimeSec = -1; // only for testing. Name server will quit after this time
  public static FailureScenario failureScenario = FailureScenario.applyActiveNameServersRunning;



  @SuppressWarnings("static-access")
  /**************************************************************
   * Initialized a command line parser
   * ***********************************************************
   */
  private static CommandLine initializeOptions(String[] args) throws ParseException {
    Option help = new Option("help", "Prints Usage");
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
    Option noLoadDB = new Option("noLoadDB", "Load items in database or not");
    Option eventualConsistency = new Option("eventualConsistency", "Eventual consistency or paxos");

    Option dataStore = new Option("dataStore", true, "Which persistent data store to use for name records");

    //Option persistentDataStore = new Option("persistentDataStore", "Use a persistent data store for name records");
    Option simpleDiskStore = new Option("simpleDiskStore", "Use a simple disk store for name records");
    Option dataFolder = new Option("dataFolder", true, "dataFolder");
    Option mongoPort = new Option("mongoPort", true, "Which port number to use for MongoDB.");


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

    Option paxosStartMinDelaySec = new Option("paxosStartMinDelaySec", true, "paxos starts at least this many seconds after start of experiment");

    Option paxosStartMaxDelaySec = new Option("paxosStartMaxDelaySec", true, "paxos starts at most this many seconds after start of experiment");

    Option emulatePingLatencies = new Option("emulatePingLatencies", "add packet delay equal to ping delay between two servers (used for emulation).");
    Option variation = new Option("variation", true, "variation");

    Option movingAverageWindowSize = OptionBuilder.withArgName("size").hasArg()
            .withDescription("Size of window to calculate the "
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
    Option signatureCheck = new Option("signatureCheck", "whether an update operation checks signature or not");
    // used for testing only

    Option quitAfterTime = new Option("quitAfterTime", true, "name server will quit after this time");

    Option nameActives = new Option("nameActives", false, "list of name actives provided to this file");

    commandLineOptions = new Options();
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
  private static final String DEFAULTAGGREGATEINTERVAL = "1000"; // seconds
  private static final String DEFAULTANALYSISINTERVAL = "1000";  // seconds
  private static final String DEFAULTNORMALIZINGCONSTANT = "0.1";
  private static final String DEFAULTMOVINGAVERAGEWINDOWSIZE = "20";
  private static final String DEFAULTTTLCONSTANT = "0.0";
  private static final String DEFAULTTTLREGULARNAME = "0";
  private static final String DEFAULTTTLMOBILENAME = "0";
  private static final String DEFAULTREGULARWORKLOADSIZE = "0";
  private static final String DEFAULTMOBILEMOBILEWORKLOADSIZE = "0";
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
    start(id, nsFile, args);
  }
  
  public static void start(int id, String nsFile, String ... args) { 
    try {
      CommandLine parser = null;
      try {
        parser = initializeOptions(args);

      } catch (ParseException e) {
        printUsage();
        System.exit(1);
        e.printStackTrace();
      }
      if (parser.hasOption("help")) {
        printUsage();
        System.exit(1);
      }
      id = Integer.parseInt(parser.getOptionValue("id", Integer.toString(id)));
      nsFile = parser.getOptionValue("nsfile", nsFile);

      GNS.numPrimaryReplicas = Integer.parseInt(parser.getOptionValue("primary", Integer.toString(GNS.DEFAULT_NUM_PRIMARY_REPLICAS)));
      aggregateInterval = Integer.parseInt(parser.getOptionValue("aInterval", DEFAULTAGGREGATEINTERVAL)) * 1000;
      analysisInterval = Integer.parseInt(parser.getOptionValue("rInterval", DEFAULTANALYSISINTERVAL)) * 1000;
      normalizingConstant = Double.parseDouble(parser.getOptionValue("nconstant", DEFAULTNORMALIZINGCONSTANT));
      movingAverageWindowSize = Integer.parseInt(parser.getOptionValue("mavg", DEFAULTMOVINGAVERAGEWINDOWSIZE));

      ttlConstant = Double.parseDouble(parser.getOptionValue("ttlconstant", DEFAULTTTLCONSTANT));
      defaultTTLRegularName = Integer.parseInt(parser.getOptionValue("rttl", DEFAULTTTLREGULARNAME));
      defaultTTLMobileName = Integer.parseInt(parser.getOptionValue("mttl", DEFAULTTTLMOBILENAME));

      regularWorkloadSize = Integer.parseInt(parser.getOptionValue("rworkload", DEFAULTREGULARWORKLOADSIZE));
      mobileWorkloadSize = Integer.parseInt(parser.getOptionValue("mworkload", DEFAULTMOBILEMOBILEWORKLOADSIZE));

//      GenerateSyntheticRecordTable.sleepBetweenNames = Integer.parseInt(parser.getOptionValue("syntheticWorkloadSleepTimeBetweenAddingNames", "0"));

      if (parser.hasOption("static")) {
        replicationFramework = ReplicationFrameworkType.STATIC;
      } else if (parser.hasOption("random")) {
        replicationFramework = ReplicationFrameworkType.RANDOM;
      } else if (parser.hasOption("location")) {
        replicationFramework = ReplicationFrameworkType.LOCATION;
        nameServerVoteSize = Integer.parseInt(parser.getOptionValue("nsVoteSize"));
      } else if (parser.hasOption("beehive")) {
        replicationFramework = ReplicationFrameworkType.BEEHIVE;
        C = Double.parseDouble(parser.getOptionValue("C"));
        base = Double.parseDouble(parser.getOptionValue("base"));
        alpha = Double.parseDouble(parser.getOptionValue("alpha"));
      } else if (parser.hasOption("kmediods")) {
        replicationFramework = ReplicationFrameworkType.KMEDIODS;
        numberLNS = Integer.parseInt(parser.getOptionValue("numLNS"));
        lnsnsPingFile = parser.getOptionValue("lnsnsping");
        nsnsPingFile = parser.getOptionValue("nsnsping");
      } else if (parser.hasOption("optimal")) {
        replicationFramework = ReplicationFrameworkType.OPTIMAL;
      } else {
        replicationFramework = GNS.DEFAULT_REPLICATION_FRAMEWORK;
        nameServerVoteSize = DEFAULT_NAMESERVER_VOTE_SIZE;
      }

      if (parser.hasOption("minReplica")) {
        minReplica = Integer.parseInt(parser.getOptionValue("minReplica"));
      }
      if (parser.hasOption("maxReplica")) {
        maxReplica = Integer.parseInt(parser.getOptionValue("maxReplica"));
      }

      debugMode = parser.hasOption("debugMode");
      experimentMode = parser.hasOption("experimentMode");
      if (experimentMode) {
        eventualConsistency = parser.hasOption("eventualConsistency");
        noLoadDB = parser.hasOption("noLoadDB");
      }

      String dataStoreString = parser.getOptionValue("dataStore");
      if (dataStoreString == null) {
        dataStore = DEFAULTDATASTORETYPE;
      } else {
        try {
          dataStore = DataStoreType.valueOf(dataStoreString);
        } catch (IllegalArgumentException e) {
          dataStore = DEFAULTDATASTORETYPE;
        }
      }
      simpleDiskStore = parser.hasOption("simpleDiskStore");


      if (simpleDiskStore && parser.hasOption("dataFolder")) {
        dataFolder = parser.getOptionValue("dataFolder");
      }
      if (parser.hasOption("mongoPort")) {
        mongoPort = Integer.parseInt(parser.getOptionValue("mongoPort"));
      }

      if (parser.hasOption("paxosLogFolder") == false) {
        PaxosManager.setPaxosLogFolder(DEFAULTPAXOSLOGPATHNAME);
      } else {
        PaxosManager.setPaxosLogFolder(parser.getOptionValue("paxosLogFolder"));
      }

      if (parser.hasOption("failureDetectionMsgInterval")) {
        PaxosManager.setFailureDetectionPingInterval(Integer.parseInt(parser.getOptionValue("failureDetectionMsgInterval")) * 1000);
      }
      if (parser.hasOption("failureDetectionTimeoutInterval")) {
        PaxosManager.setFailureDetectionTimeoutInterval(Integer.parseInt(parser.getOptionValue("failureDetectionTimeoutInterval")) * 1000);
      }

      if (experimentMode) {
        paxosStartMinDelaySec = Integer.parseInt(parser.getOptionValue("paxosStartMinDelaySec", "0"));
        paxosStartMaxDelaySec = Integer.parseInt(parser.getOptionValue("paxosStartMaxDelaySec", "0"));
      }


      if (parser.hasOption("emulatePingLatencies")) {
        emulatePingLatencies = parser.hasOption("emulatePingLatencies");
        if (emulatePingLatencies && parser.hasOption("variation")) {
          variation = Double.parseDouble(parser.getOptionValue("variation"));
        }
      }

      if (parser.hasOption("workerThreadCount")) {
        workerThreadCount = Integer.parseInt(parser.getOptionValue("workerThreadCount"));
      }

      tinyUpdate = parser.hasOption("tinyUpdate");

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

      // only for testing
      if (experimentMode) {
        if (parser.hasOption("quitAfterTime")) {
          quitAfterTimeSec = Integer.parseInt(parser.getOptionValue("quitAfterTime"));
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
        nameActives = parser.getOptionValue("nameActives", null);
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
            "Default TTL Regular Names: " + defaultTTLRegularName, debugMode);
    println(
            "Default TTL Mobile Names: " + defaultTTLMobileName, debugMode);
    println(
            "Debug Mode: " + debugMode, debugMode);
    println(
            "Experiment Mode: " + experimentMode, debugMode);

    try {
      //Generate name server lookup table
      ConfigFileInfo.readHostInfo(nsFile, id);
      HashFunction.initializeHashFunction();
      //Start nameserver 
      new NameServer(id).run();
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
    if (NameServer.nodeID == StartNameServer.quitNodeID && failureScenario.equals(StartNameServer.failureScenario)) {
      GNS.getLogger().severe("SYSTEM EXIT. Failure Scenario. " + failureScenario);
      System.exit(2);
    }
  }
}
