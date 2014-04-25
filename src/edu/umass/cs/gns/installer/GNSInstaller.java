package edu.umass.cs.gns.installer;

import edu.umass.cs.networktools.SSHClient;
import edu.umass.cs.gns.database.DataStoreType;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.GNSNodeConfig;
import edu.umass.cs.gns.statusdisplay.StatusEntry;
import edu.umass.cs.gns.statusdisplay.StatusListener;
import edu.umass.cs.gns.statusdisplay.StatusModel;
import edu.umass.cs.gns.util.Format;
import edu.umass.cs.networktools.ExecuteBash;
import java.io.File;
import java.net.InetAddress;
import java.net.URISyntaxException;
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
 * Installs n instances of the GNS Jars and executes them.
 *
 * Typical use:
 *
 * java -cp GNS.jar edu.umass.cs.gns.installer.GNSInstaller -create gns_dev
 *
 * Where gns_dev is an xml formatted configuration file that looks something like this:
 * <code>
 * <root>
 * <ec2username name="ec2-user"/>
 * <keyname name="aws"/>
 * <hosttype name="linux"/>
 * <datastore name="MONGO"/>
 * <host id="0" ip="127.0.0.1"/>
 * <host id="1" ip="127.0.0.2"/>
 * <host id="2" ip="127.0.0.3"/>
 * </root>
 * </code>
 *
 * @author westy
 */
public class GNSInstaller {

  private static final String LNS_CONF_FILE = "/conf/lns.conf";
  private static final String NS_CONF_FILE = "/conf/ns.conf";
  private static final String NEWLINE = System.getProperty("line.separator");
  private static final String FILESEPARATOR = System.getProperty("file.separator");
  private static final String PRIVATEKEYFILEEXTENSION = ".pem";
  private static final String KEYHOME = System.getProperty("user.home") + FILESEPARATOR + ".ssh";
  private static final DataStoreType DEFAULT_DATA_STORE_TYPE = DataStoreType.MONGO;
  private static final String DEFAULT_EC2_USERNAME = "ec2-user";
  private static final String DEFAULT_KEYNAME = "aws";
  /**
   * Stores information about the hosts we're using.
   */
  private static ConcurrentHashMap<Integer, HostInfo> hostTable = new ConcurrentHashMap<Integer, HostInfo>();
  //
  private static DataStoreType dataStoreType = DEFAULT_DATA_STORE_TYPE;
  private static String hostType = "linux";
  private static String ec2UserName = DEFAULT_EC2_USERNAME;
  private static String keyName = DEFAULT_KEYNAME;

  private static String gnsJarFileLocation;
  private static String nsConfFileLocation;
  private static String lnsConfFileLocation;
  private static String gnsFileName;
  private static String lnsConfFileName;
  private static String nsConfFileName;

  private static void loadConfig(String configName) {
    HostConfigParser parser = new HostConfigParser(configName);
    for (HostInfo hostInfo : parser.getHosts()) {
      hostTable.put(hostInfo.getId(), hostInfo);
    }
    keyName = parser.getKeyname();
    ec2UserName = parser.getUsername();
    dataStoreType = parser.getDataStoreType();
    hostType = parser.getHostType();
  }

  /**
   * Copies the latest version of the JAR files to the all the hosts in the runset given by name and restarts all the servers.
   *
   * @param name
   * @param action
   */
  public static void updateRunSet(String name, UpdateAction action, boolean removeLogs, boolean deleteDatabase, boolean firstInstall) {
    ArrayList<Thread> threads = new ArrayList<Thread>();
    for (HostInfo info : hostTable.values()) {
      threads.add(new UpdateThread(info.getHostname(), info.getId(), action, removeLogs, deleteDatabase, firstInstall));
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
    updateNodeConfigAndSendOutServerInit();
  }

  public enum UpdateAction {

    UPDATE,
    RESTART,
  };

  /**
   * This is called to install and run the GNS on a single host. This is called concurrently in
   * one thread per each host.
   * Copies the JAR and conf files and optionally resets some other stuff depending on the
   * update action given.
   * The name-server-info file is created using all the IP address of all the hosts.
   * Then the various servers are started on the host.
   *
   * @param id
   * @param hostname
   * @param action
   */
  public static void updateAndRunGNS(int id, String hostname, UpdateAction action, boolean removeLogs, boolean deleteDatabase, boolean firstInstall) {
    System.out.println("**** Node " + id + " running on " + hostname + " starting update ****");
    killAllServers(id, hostname);
    if (removeLogs) {
      removeLogFiles(id, hostname);
    }
    if (deleteDatabase) {
      deleteDatabase(id, hostname);
    }
    switch (action) {
      case UPDATE:
        copyJarAndConfFiles(id, hostname);
        break;
      case RESTART:
        break;
    }
    // write the name-server-info
    if (firstInstall) {
      writeNSFile(hostname);
    }
    startServers(id, hostname);
    System.out.println("#### Node " + id + " running on " + hostname + " finished update ####");
  }

  /**
   * Starts an LNS, NS server on the remote host.
   *
   * @param id
   * @param hostname
   */
  private static void startServers(int id, String hostname) {
    StatusModel.getInstance().queueUpdate(id, "Starting local name servers");
    File keyFile = getKeyFile();
    ExecuteBash.executeBashScript(hostname, keyFile, "runLNS.sh",
            "#!/bin/bash\n"
            + "cd /home/ec2-user\n"
            + "if [ -f LNSlogfile ]; then\n"
            + "mv --backup=numbered LNSlogfile LNSlogfile.save\n"
            + "fi\n"
            + "nohup java -cp " + gnsFileName + " " + StartLNSClass + " "
            + "-id " + getLNSId(id)
            + " -configFile lns.conf "
            + "> LNSlogfile 2>&1 &");

    StatusModel.getInstance().queueUpdate(id, "Starting name servers");
    ExecuteBash.executeBashScript(hostname, keyFile, "runNS.sh",
            "#!/bin/bash\n"
            + "cd /home/ec2-user\n"
            + "if [ -f NSlogfile ]; then\n"
            + "mv --backup=numbered NSlogfile NSlogfile.save\n"
            + "fi\n"
            + "nohup java -cp " + gnsFileName + " " + StartNSClass + " "
            + " -id " + id
            + " -configFile ns.conf "
            + "> NSlogfile 2>&1 &");
    StatusModel.getInstance().queueUpdate(id, StatusEntry.State.RUNNING, "All servers started");
  }

  /**
   * Copies the JAR and configuration files to the remote host.
   *
   * @param id
   * @param hostname
   */
  private static void copyJarAndConfFiles(int id, String hostname) {
    File keyFile = getKeyFile();
    StatusModel.getInstance().queueUpdate(id, "Copying jar and conf files");
    SSHClient.scpTo(ec2UserName, hostname, keyFile, gnsJarFileLocation, gnsFileName);
    SSHClient.scpTo(ec2UserName, hostname, keyFile, lnsConfFileLocation, lnsConfFileName);
    SSHClient.scpTo(ec2UserName, hostname, keyFile, nsConfFileLocation, nsConfFileName);
  }
  private static final String MongoRecordsClass = "edu.umass.cs.gns.database.MongoRecords";
  private static final String CassandraRecordsClass = "edu.umass.cs.gns.database.CassandraRecords";

  /**
   * Deletes the database on the remote host.
   *
   * @param id
   * @param hostname
   */
  private static void deleteDatabase(int id, String hostname) {
    ExecuteBash.executeBashScript(hostname, getKeyFile(), "deleteDatabase.sh",
            "#!/bin/bash\n"
            + "java -cp " + gnsFileName + " " + MongoRecordsClass + " -clear");
  }
  private static final String StartLNSClass = "edu.umass.cs.gns.main.StartLocalNameServer";
  private static final String StartNSClass = "edu.umass.cs.gns.main.StartNameServer";

  // assumes one LNS for each NS with with number starting at NS count.
  private static int getLNSId(int id) {
    return id + hostTable.size();
  }

  /**
   * Kills all servers on the remote host.
   *
   * @param id
   * @param hostname
   */
  private static void killAllServers(int id, String hostname) {
    StatusModel.getInstance().queueUpdate(id, "Killing servers");
    ExecuteBash.executeBashScript(hostname, getKeyFile(), "killAllServers.sh", "#!/bin/bash\nkillall java");
  }

  /**
   * Removes log files on the remote host.
   *
   * @param id
   * @param hostname
   */
  private static void removeLogFiles(int id, String hostname) {
    StatusModel.getInstance().queueUpdate(id, "Removing log files");
    ExecuteBash.executeBashScript(hostname, getKeyFile(), "removelogs.sh", "#!/bin/bash\n"
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
    StringBuilder result = new StringBuilder();
    //HostID IsNS? IPAddress [StartingPort | - ] Ping-Latency Latitude Longitude
    // WRITE OUT NSs
    for (HostInfo info : hostTable.values()) {
      result.append(info.getId());
      result.append(" yes ");
      result.append(info.getHostname());
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
      result.append(info.getHostname());
      result.append(" default ");
      result.append(" 0 ");
      result.append(info.getLocation() != null ? Format.formatLatLong(info.getLocation().getY()) : 0.0);
      result.append(" ");
      result.append(info.getLocation() != null ? Format.formatLatLong(info.getLocation().getX()) : 0.0);
      result.append(NEWLINE);
    }

    SSHClient.execWithSudoNoPass(ec2UserName, hostname, getKeyFile(), "echo \"" + result.toString() + "\" > name-server-info");
  }

  // Probably unnecessary at this point.
  private static void updateNodeConfigAndSendOutServerInit() {
    GNSNodeConfig nodeConfig = new GNSNodeConfig();
    // update the config info so know where to send stuff
    try {
      for (HostInfo info : hostTable.values()) {
        InetAddress ipAddress = InetAddress.getByName(info.getHostname());
        nodeConfig.addHostInfo(info.getId(), ipAddress, GNS.STARTINGPORT, 0, info.getLocation().getY(), info.getLocation().getX());
      }
    } catch (UnknownHostException e) {
      System.err.println("Problem parsing IP address " + e);
    }
    // now we send out packets telling all the hosts where to send their status updates
    StatusListener.sendOutServerInitPackets(nodeConfig, hostTable.keySet());
  }

  /**
   * Figures out the locations of the JAR and conf files.
   *
   * @return true if it found them
   */
  private static boolean setupJarAndConfFilePaths() {
    File jarPath = getJarPath();
    System.out.println("Jar path: " + jarPath);
    gnsJarFileLocation = jarPath.getPath();
    String distFolderLocation = jarPath.getParent();
    lnsConfFileLocation = distFolderLocation + LNS_CONF_FILE;
    nsConfFileLocation = distFolderLocation + NS_CONF_FILE;
    System.out.println("LNS conf: " + lnsConfFileLocation);
    System.out.println("NS conf: " + nsConfFileLocation);
    if (!new File(lnsConfFileLocation).exists() && !new File(nsConfFileLocation).exists()) {
      return false;
    }
    gnsFileName = new File(gnsJarFileLocation).getName();
    lnsConfFileName = new File(lnsConfFileLocation).getName();
    nsConfFileName = new File(nsConfFileLocation).getName();
    return true;
  }

  /**
   * Returns the location of the JAR that is running.
   *
   * @return the path
   */
  private static File getJarPath() {
    try {
      return new File(GNS.class.getProtectionDomain().getCodeSource().getLocation().toURI());
    } catch (URISyntaxException e) {
      GNS.getLogger().info("Unable to get jar location: " + e);
      return null;
    }
  }

  /**
   * Returns the location of the key file (probably in the users .ssh home).
   *
   * @return
   */
  private static File getKeyFile() {
    return new File(KEYHOME + FILESEPARATOR + keyName + PRIVATEKEYFILEEXTENSION);
  }

  // COMMAND LINE STUFF
  private static HelpFormatter formatter = new HelpFormatter();
  private static Options commandLineOptions;

  private static CommandLine initializeOptions(String[] args) throws ParseException {
    Option help = new Option("help", "Prints Usage");
    Option install = OptionBuilder.withArgName("runSet name").hasArg()
            .withDescription("installs GNS files and starts servers in a runset")
            .create("install");
    Option update = OptionBuilder.withArgName("runSet name").hasArg()
            .withDescription("updates GNS files and restarts servers in a runset")
            .create("update");
    Option restart = OptionBuilder.withArgName("runSet name").hasArg()
            .withDescription("restarts GNS servers in a runset")
            .create("restart");
    Option removeLogs = new Option("removeLogs", "remove paxos and Logger log files (use with -restart or -update)");
    Option deleteDatabase = new Option("deleteDatabase", "delete the databases in a runset (use with -restart or -update)");
    Option dataStore = OptionBuilder.withArgName("data store type").hasArg()
            .withDescription("data store type")
            .create("datastore");

    commandLineOptions = new Options();
    commandLineOptions.addOption(install);
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

  public static void main(String[] args) {
    try {
      CommandLine parser = initializeOptions(args);
      if (parser.hasOption("help")) {
        printUsage();
        System.exit(1);
      }
      String runsetInstall = parser.getOptionValue("install");
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

      String configName = runsetInstall != null ? runsetInstall
              : runsetUpdate != null ? runsetUpdate
              : runsetRestart != null ? runsetRestart
              : null;

      System.out.println("Config name: " + configName);
      if (configName != null) {
        loadConfig(configName);
      }

      if (!setupJarAndConfFilePaths()) {
        System.out.println("Can't locate needed config files. LNS conf: " + lnsConfFileLocation + " NS conf: " + nsConfFileLocation);
        System.exit(1);
      }

      ExecuteBash.setEc2Username(ec2UserName);
      SSHClient.setVerbose(true);

      boolean isFirstInstall = false;
      // install is the same as update except we don't have to do a few things
      if (runsetInstall != null) {
        runsetUpdate = runsetInstall;
        isFirstInstall = true;
      }

      if (runsetUpdate != null) {
        updateRunSet(runsetUpdate, UpdateAction.UPDATE, removeLogs, deleteDatabase, isFirstInstall);
      } else if (runsetRestart != null) {
        updateRunSet(runsetUpdate, UpdateAction.RESTART, removeLogs, deleteDatabase, false);
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

  /**
   * The thread we use to run a copy of the updater for each host we're updating.
   */
  static class UpdateThread extends Thread {

    private String hostname;
    private int id;
    private UpdateAction action;
    boolean removeLogs;
    boolean deleteDatabase;
    private boolean firstInstall;

    public UpdateThread(String hostname, int id, UpdateAction action, boolean removeLogs, boolean deleteDatabase, boolean firstInstall) {
      this.hostname = hostname;
      this.id = id;
      this.action = action;
      this.removeLogs = removeLogs;
      this.deleteDatabase = deleteDatabase;
      this.firstInstall = firstInstall;
    }

    @Override
    public void run() {
      GNSInstaller.updateAndRunGNS(id, hostname, action, removeLogs, deleteDatabase, firstInstall);
    }
  }

}
