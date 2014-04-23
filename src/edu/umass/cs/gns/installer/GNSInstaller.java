package edu.umass.cs.gns.installer;

import edu.umass.cs.amazontools.AWSEC2;
import edu.umass.cs.networktools.SSHClient;
import edu.umass.cs.gns.database.DataStoreType;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.GNSNodeConfig;
import edu.umass.cs.gns.statusdisplay.StatusEntry;
import edu.umass.cs.gns.statusdisplay.StatusListener;
import edu.umass.cs.gns.statusdisplay.StatusModel;
import edu.umass.cs.gns.util.Format;
import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * install n instances of the jars - create a runSet
 *
 * tag each instance with the runSet name
 *
 * exec commands on each of the instances in the runSet
 *
 *
 */
/**
 * Typical use:
 *
 * java -cp GNS.jar edu.umass.cs.gns.installer.GNSInstaller -create gns_dev
 *
 * @author westy
 */
public class GNSInstaller {

  // this is the only hardcoded thing in here you should have to change...
  // Note: figure out a better way to do this.. probably with resources
  private static final String DIST_FOLDER_LOCATION = "/Users/westy/Documents/Code/GNS/dist";
  private static final String NEWLINE = System.getProperty("line.separator");
  private static final String FILESEPARATOR = System.getProperty("file.separator");
  private static final String PRIVATEKEYFILEEXTENSION = ".pem";
  private static final String KEYHOME = System.getProperty("user.home") + FILESEPARATOR + ".ssh";
  private static final DataStoreType DEFAULT_DATA_STORE_TYPE = DataStoreType.MONGO;
  private static final String DEFAULT_EC2_USERNAME = "ec2-user";
  /**
   * Stores information about instances hosts we're using.
   */
  private static ConcurrentHashMap<Integer, HostInfo> hostTable = new ConcurrentHashMap<Integer, HostInfo>();
  //
  private static ConcurrentHashMap<Integer, Integer> hostsThatDidNotStart = new ConcurrentHashMap<Integer, Integer>();
  private static DataStoreType dataStoreType = DEFAULT_DATA_STORE_TYPE;
  private static String hostType = "linux";
  private static String ec2UserName = DEFAULT_EC2_USERNAME;

  private static void loadConfig(String configName) {
    HostConfigParser parser = new HostConfigParser(configName);
    for (HostInfo hostInfo : parser.getHosts()) {
      hostTable.put(hostInfo.getId(), hostInfo);
    }
    ec2UserName = parser.getEc2username();
    dataStoreType = parser.getDataStoreType();
    hostType = parser.getHostType();
  }

  /**
   * Starts the GNS on a set of hosts.
   *
   * @param configName
   */
  public static void startGnsOnHosts(String configName) {
    // got a complete set running... now on to step 2
    System.out.println(hostTable.toString());
    // after we know all the hosts are we run the last part
    ArrayList<Thread> threads = new ArrayList<Thread>();
    // now start all the finishing threads
    for (HostInfo info : hostTable.values()) {
      System.out.println("Installing on " + info.getIp());
      //GNS.getLogger().info("Finishing install for " + entry.getKey());
      threads.add(new InstallFinishThread(info.getId(), info.getIp()));
    }
    for (Thread thread : threads) {
      thread.start();
    }
    // and wait form the to complete
    try {
      for (Thread thread : threads) {
        thread.join();
      }
    } catch (InterruptedException e) {
      System.out.println("Problem joining threads: " + e);
    }
    System.out.println("Finished creation of Host Set " + configName);

    GNSNodeConfig nodeConfig = new GNSNodeConfig();
    // update the config info so know where to send stuff
    try {
      for (HostInfo info : hostTable.values()) {
        InetAddress ipAddress = InetAddress.getByName(info.getIp());
        nodeConfig.addHostInfo(info.getId(), ipAddress, GNS.STARTINGPORT, 0, info.getLocation().getY(), info.getLocation().getX());
      }
    } catch (UnknownHostException e) {
      System.err.println("Problem parsing IP address " + e);
    }
    // now we send out packets telling all the hosts where to send their status updates
    StatusListener.sendOutServerInitPackets(nodeConfig, hostTable.keySet());
  }
  
  // Code for copying JARS and starting stuff
  
  private static final String keyName = "aws";
  private static final String GNSJarFileLocation = DIST_FOLDER_LOCATION + "/GNS.jar";
  private static final String lnsConfFileLocation = DIST_FOLDER_LOCATION + "/conf/lns.conf";
  private static final String nsConfFileLocation = DIST_FOLDER_LOCATION + "/conf/ns.conf";
  private static String GNSFileName = new File(GNSJarFileLocation).getName();
  private static String lnsConfFileName = new File(lnsConfFileLocation).getName();
  private static String nsConfFileName = new File(nsConfFileLocation).getName();

  /**
   * This is called to install and run the GNS on a bunch of hosts. The name-server-info file is created using all
   * the IP address of all the hosts. Then the various servers are started on the host.
   *
   * @param id
   * @param hostname
   */
  public static void installAndRunGNS(int id, String hostname) {
    // move the JAR files over
    copyJARAndConfFiles(id, hostname);
    // write the name-server-info
    StatusModel.getInstance().queueUpdate(id, "Creating name-server-info");
    writeNSFile(hostname);
    startServers(id, hostname);
  }

  private static void copyJARAndConfFiles(int id, String hostname) {
    File keyFile = new File(KEYHOME + FILESEPARATOR + keyName + PRIVATEKEYFILEEXTENSION);
    StatusModel.getInstance().queueUpdate(id, "Copying jar and conf files");
    // this is simple, but figure out a smarter way to do this without hardcoded source folder location
    SSHClient.scpTo(ec2UserName, hostname, keyFile, GNSJarFileLocation, GNSFileName);
    SSHClient.scpTo(ec2UserName, hostname, keyFile, lnsConfFileLocation, lnsConfFileName);
    SSHClient.scpTo(ec2UserName, hostname, keyFile, nsConfFileLocation, nsConfFileName);
  }
  private static final String MongoRecordsClass = "edu.umass.cs.gns.database.MongoRecords";
  private static final String CassandraRecordsClass = "edu.umass.cs.gns.database.CassandraRecords";

  private static void deleteDatabase(int id, String hostname) {
    File keyFile = new File(KEYHOME + FILESEPARATOR + keyName + PRIVATEKEYFILEEXTENSION);
    AWSEC2.executeBashScript(hostname, keyFile, "deleteDatabase.sh",
            "#!/bin/bash\n"
            + "java -cp " + GNSFileName + " " + MongoRecordsClass + " -clear");
  }
  private static final String StartLNSClass = "edu.umass.cs.gns.main.StartLocalNameServer";
  private static final String StartNSClass = "edu.umass.cs.gns.main.StartNameServer";
  // unused
  //private static final String StartHTTPServerClass = "edu.umass.cs.gns.httpserver.GnsHttpServer";

  // assumes one LNS for each NS with with number starting at NS count.
  private static int getLNSId(int id) {
    return id + hostTable.size();
  }

  /**
   * Starts an LNS, NS server on the host.
   *
   * @param id
   * @param hostname
   */
  private static void startServers(int id, String hostname) {
    StatusModel.getInstance().queueUpdate(id, "Starting local name servers");
    File keyFile = new File(KEYHOME + FILESEPARATOR + keyName + PRIVATEKEYFILEEXTENSION);
    AWSEC2.executeBashScript(hostname, keyFile, "runLNS.sh",
            "#!/bin/bash\n"
            + "cd /home/ec2-user\n"
            + "if [ -f LNSlogfile ]; then\n"
            + "mv --backup=numbered LNSlogfile LNSlogfile.save\n"
            + "fi\n"
            + "nohup java -cp " + GNSFileName + " " + StartLNSClass + " "
            + "-id " + getLNSId(id)
            + " -configFile lns.conf "
            + "> LNSlogfile 2>&1 &");

    StatusModel.getInstance().queueUpdate(id, "Starting name servers");
    AWSEC2.executeBashScript(hostname, keyFile, "runNS.sh",
            "#!/bin/bash\n"
            + "cd /home/ec2-user\n"
            + "if [ -f NSlogfile ]; then\n"
            + "mv --backup=numbered NSlogfile NSlogfile.save\n"
            + "fi\n"
            + "nohup java -cp " + GNSFileName + " " + StartNSClass + " "
            + " -id " + id
            + " -configFile ns.conf "
            + "> NSlogfile 2>&1 &");
    StatusModel.getInstance().queueUpdate(id, StatusEntry.State.RUNNING, "All servers started");
  }

  private static void killAllServers(int id, String hostname) {
    File keyFile = new File(KEYHOME + FILESEPARATOR + keyName + PRIVATEKEYFILEEXTENSION);
    StatusModel.getInstance().queueUpdate(id, "Killing servers");
    AWSEC2.executeBashScript(hostname, keyFile, "killAllServers.sh", "#!/bin/bash\nkillall java");
  }

  private static void removeLogFiles(int id, String hostname) {
    File keyFile = new File(KEYHOME + FILESEPARATOR + keyName + PRIVATEKEYFILEEXTENSION);
    StatusModel.getInstance().queueUpdate(id, "Removing log files");
    AWSEC2.executeBashScript(hostname, keyFile, "removelogs.sh", "#!/bin/bash\n"
            + "rm NSlogfile*\n"
            + "rm LNSlogfile*\n"
            + "rm -rf log\n"
            + "rm -rf paxoslog");
  }

  /**
   * Write the name-server-info file on the remote host.
   *
   * @param hostname
   * @param keyFile
   */
  private static void writeNSFile(String hostname) {
    File keyFile = new File(KEYHOME + FILESEPARATOR + keyName + PRIVATEKEYFILEEXTENSION);
    StringBuilder result = new StringBuilder();
    //HostID IsNS? IPAddress [StartingPort | - ] Ping-Latency Latitude Longitude
    // WRITE OUT NSs
    for (HostInfo info : hostTable.values()) {
      result.append(info.getId());
      result.append(" yes ");
      result.append(info.getIp());
      result.append(" default ");
      result.append(" 0 ");
      result.append(info.getLocation() != null ? Format.formatLatLong(info.getLocation().getY()) : 0.0);
      result.append(" ");
      result.append(info.getLocation() != null ? Format.formatLatLong(info.getLocation().getX()) : 0.0);
      result.append(NEWLINE);
    }
    // WRITE OUT LNSs whose numbers are N above NSs where N is the number of NSs
    for (HostInfo info : hostTable.values()) {
      result.append(getLNSId(info.getId()));
      result.append(" no ");
      result.append(info.getIp());
      result.append(" default ");
      result.append(" 0 ");
      result.append(info.getLocation() != null ? Format.formatLatLong(info.getLocation().getY()) : 0.0);
      result.append(" ");
      result.append(info.getLocation() != null ? Format.formatLatLong(info.getLocation().getX()) : 0.0);
      result.append(NEWLINE);
    }

    SSHClient.execWithSudoNoPass(ec2UserName, hostname, keyFile, "echo \"" + result.toString() + "\" > name-server-info");
  }

  public enum UpdateAction {

    UPDATE,
    REMOVE_LOGS_AND_UPDATE,
    REMOVE_LOGS_AND_DELETE_DATABASE_AND_UPDATE,
    DELETE_DATABASE_AND_UPDATE,
    RESTART,
    REMOVE_LOGS_AND_RESTART,
    REMOVE_LOGS_AND_DELETE_DATABASE_AND_RESTART,
    DELETE_DATABASE_AND_RESTART
  };

  /**
   * Copies the latest version of the JAR files to the all the hosts in the runset given by name and restarts all the servers.
   *
   * @param name
   * @param action
   */
  public static void updateRunSet(String name, UpdateAction action) {
    ArrayList<Thread> threads = new ArrayList<Thread>();
    for (HostInfo info : hostTable.values()) {
      threads.add(new UpdateThread(info.getId(), info.getIp(), action));
    }
    for (Thread thread : threads) {
      thread.start();
    }
    // and wait for them to complete
    try {
      for (Thread thread : threads) {
        thread.join();
      }
    } catch (InterruptedException e) {
      System.out.println("Problem joining threads: " + e);
    }
  }

  // COMMAND LINE STUFF
  private static HelpFormatter formatter = new HelpFormatter();
  private static Options commandLineOptions;

  private static CommandLine initializeOptions(String[] args) throws ParseException {
    Option help = new Option("help", "Prints Usage");
    Option update = OptionBuilder.withArgName("runSet name").hasArg()
            .withDescription("update a runset")
            .create("update");
    Option restart = OptionBuilder.withArgName("runSet name").hasArg()
            .withDescription("restart a runset")
            .create("restart");
    Option removeLogs = new Option("removeLogs", "remove paxos and Logger log files (use with -restart or -update)");
    Option deleteDatabase = new Option("deleteDatabase", "delete the databases in a runset (use with -restart or -update)");
    Option dataStore = OptionBuilder.withArgName("data store type").hasArg()
            .withDescription("data store type")
            .create("datastore");

    commandLineOptions = new Options();
    commandLineOptions.addOption(update);
    commandLineOptions.addOption(restart);
    commandLineOptions.addOption(removeLogs);
    commandLineOptions.addOption(deleteDatabase);
    commandLineOptions.addOption(dataStore);
    commandLineOptions.addOption(help);

    CommandLineParser parser = new GnuParser();
    return parser.parse(commandLineOptions, args);
  }

  private static void printUsage() {
    formatter.printHelp("java -cp GNS.jar edu.umass.cs.gns.main.EC2Installer <options>", commandLineOptions);
  }

  private static void printUsage(String header) {
    formatter.printHelp("java -cp GNS.jar edu.umass.cs.gns.main.EC2Installer <options>", header, commandLineOptions, "");
  }

  public static void main(String[] args) {
    try {
      CommandLine parser = initializeOptions(args);
      if (parser.hasOption("help")) {
        printUsage();
        System.exit(1);
      }
      String runsetUpdate = parser.getOptionValue("update");
      String runsetRestart = parser.getOptionValue("restart");
      String dataStoreName = parser.getOptionValue("datastore");
      boolean removeLogs = parser.hasOption("removeLogs");
      boolean deleteDatabase = parser.hasOption("deleteDatabase");

      if (dataStoreName != null) {
        try {
          dataStoreType = DataStoreType.valueOf(dataStoreName);
        } catch (IllegalArgumentException e) {
          System.out.println("Unknown data store type " + dataStoreName + "; exiting.");
          System.exit(1);
        }
      }

      String configName = runsetUpdate != null ? runsetUpdate
              : runsetRestart != null ? runsetRestart : null;

      System.out.println("Config name: " + configName);
      if (configName != null) {
        loadConfig(configName);
      }

      if (runsetUpdate != null) {
        updateRunSet(runsetUpdate, (removeLogs
                ? (deleteDatabase ? UpdateAction.REMOVE_LOGS_AND_DELETE_DATABASE_AND_UPDATE : UpdateAction.REMOVE_LOGS_AND_UPDATE)
                : (deleteDatabase ? UpdateAction.DELETE_DATABASE_AND_UPDATE : UpdateAction.UPDATE)));
      } else if (runsetRestart != null) {
        updateRunSet(runsetRestart, (removeLogs
                ? (deleteDatabase ? UpdateAction.REMOVE_LOGS_AND_DELETE_DATABASE_AND_RESTART : UpdateAction.REMOVE_LOGS_AND_RESTART)
                : (deleteDatabase ? UpdateAction.DELETE_DATABASE_AND_RESTART : UpdateAction.RESTART)));
      } else {
        printUsage();
        System.exit(1);
      }

    } catch (ParseException e1) {
      e1.printStackTrace();
      printUsage();
      System.exit(1);
    }
    System.exit(0);
  }

  static class InstallFinishThread extends Thread {

    String hostname;
    int id;

    public InstallFinishThread(int id, String hostname) {
      super("Install Finish " + id);
      this.hostname = hostname;
      this.id = id;
    }

    @Override
    public void run() {
      GNSInstaller.installAndRunGNS(id, hostname);
    }
  }

  static class UpdateThread extends Thread {

    String hostname;
    int id;
    UpdateAction action;

    public UpdateThread(int id, String hostname, UpdateAction action) {
      super("Update " + id);
      this.hostname = hostname;
      this.id = id;
      this.action = action;
    }

    @Override
    public void run() {
      System.out.println("**** Node " + id + " running on " + hostname + " starting update ****");
      killAllServers(id, hostname);
      switch (action) {
        case UPDATE:
          copyJARAndConfFiles(id, hostname);
          break;
        case REMOVE_LOGS_AND_UPDATE:
          removeLogFiles(id, hostname);
          copyJARAndConfFiles(id, hostname);
          break;
        case DELETE_DATABASE_AND_UPDATE:
          deleteDatabase(id, hostname);
          copyJARAndConfFiles(id, hostname);
          break;
        case REMOVE_LOGS_AND_DELETE_DATABASE_AND_UPDATE:
          removeLogFiles(id, hostname);
          deleteDatabase(id, hostname);
          copyJARAndConfFiles(id, hostname);
          break;
        case RESTART:
          break;
        case REMOVE_LOGS_AND_RESTART:
          removeLogFiles(id, hostname);
          break;
        case DELETE_DATABASE_AND_RESTART:
          deleteDatabase(id, hostname);
          break;
        case REMOVE_LOGS_AND_DELETE_DATABASE_AND_RESTART:
          removeLogFiles(id, hostname);
          deleteDatabase(id, hostname);
          break;
      }
      // write the name-server-info
      writeNSFile(hostname);
      startServers(id, hostname);
      System.out.println("#### Node " + id + " running on " + hostname + " finished update ####");
    }
  }
}
