package edu.umass.cs.gns.installer;

import edu.umass.cs.aws.networktools.ExecuteBash;
import edu.umass.cs.aws.networktools.RSync;
import edu.umass.cs.aws.networktools.SSHClient;
import edu.umass.cs.gns.database.DataStoreType;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.GNSNodeConfig;
import edu.umass.cs.gns.statusdisplay.StatusListener;
import edu.umass.cs.gns.util.Format;
import java.io.BufferedWriter;
import org.apache.commons.cli.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Installs n instances of the GNS Jars and executes them.
 *
 * Typical use:
 *
 * java -cp GNS.jar edu.umass.cs.gns.installer.GNSInstaller -update gns_dev
 *
 *
 * @author westy
 */
public class GNSInstallerV2 {

  private static final String NEWLINE = System.getProperty("line.separator");
  private static final String FILESEPARATOR = System.getProperty("file.separator");
  private static final String CONF_FOLDER = FILESEPARATOR + "conf";
  private static final String LNS_CONF_FILE = CONF_FOLDER + FILESEPARATOR + "lns.conf";
  private static final String NS_CONF_FILE = CONF_FOLDER + FILESEPARATOR + "ns.conf";
  private static final String PRIVATEKEYFILEEXTENSION = ".pem";
  private static final String KEYHOME = System.getProperty("user.home") + FILESEPARATOR + ".ssh";
  public static final DataStoreType DEFAULT_DATA_STORE_TYPE = DataStoreType.MONGO;
  private static final String DEFAULT_USERNAME = "ec2-user";
  private static final String DEFAULT_KEYNAME = "id_rsa";
  private static final String DEFAULT_INSTALL_PATH = "gns";
  /**
   * Stores information about the hosts we're using.
   */
  private static ConcurrentHashMap<Integer, HostInfo> hostTable = new ConcurrentHashMap<Integer, HostInfo>();
  //
  private static DataStoreType dataStoreType = DEFAULT_DATA_STORE_TYPE;
  private static String hostType = "linux";
  private static String userName = DEFAULT_USERNAME;
  private static String keyName = DEFAULT_KEYNAME;
  private static String installPath = DEFAULT_INSTALL_PATH;

  private static String distFolderLocation;
  private static String gnsJarFileLocation;
  private static String nsConfFileLocation;
  private static String lnsConfFileLocation;
  private static String gnsFileName;
  private static String lnsConfFileName;
  private static String nsConfFileName;

  private static boolean loadConfig(String configName) {
    try {
      HostConfigParser parser = new HostConfigParser(configName);
      for (HostInfo hostInfo : parser.getHosts()) {
        hostTable.put(hostInfo.getId(), hostInfo);
      }
      keyName = parser.getKeyname();
      System.out.println("Key Name: " + keyName);
      userName = parser.getUsername();
      System.out.println("User Name: " + userName);
      dataStoreType = parser.getDataStoreType();
      System.out.println("Data Store Type: " + dataStoreType);
      hostType = parser.getHostType();
      System.out.println("Host Type: " + hostType);
      installPath = parser.getInstallPath();
      if (installPath == null) {
        installPath = DEFAULT_INSTALL_PATH;
      }
      System.out.println("Install Path: " + installPath);
      return true;
    } catch (HostConfigParseException e) {
      System.out.println("Problem loading config file: " + e);
      return false;
    }
  }

  /**
   * Copies the latest version of the JAR files to the all the hosts in the runset given by name and restarts all the servers.
   *
   * @param name
   * @param action
   */
  public static void updateRunSet(String name, InstallerAction action, boolean removeLogs, boolean deleteDatabase, String scriptFile) {
    ArrayList<Thread> threads = new ArrayList<Thread>();
    for (HostInfo info : hostTable.values()) {
      threads.add(new UpdateThread(info.getHostname(), info.getId(), action, removeLogs, deleteDatabase, scriptFile));
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
    if (action != InstallerAction.STOP) {
      updateNodeConfigAndSendOutServerInit();
    }
  }

  public enum InstallerAction {

    UPDATE,
    RESTART,
    STOP,
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
   * @param removeLogs
   * @param deleteDatabase
   * @param scriptFile
   * @throws java.net.UnknownHostException
   */
  public static void updateAndRunGNS(int id, String hostname, InstallerAction action, boolean removeLogs,
          boolean deleteDatabase, String scriptFile) throws UnknownHostException {
    if (!action.equals(InstallerAction.STOP)) {
      System.out.println("**** Node " + id + " running on " + hostname + " starting update ****");
      makeInstallDir(hostname);
      killAllServers(hostname);
      if (scriptFile != null) {
        executeScriptFile(id, hostname, scriptFile);
      }
      if (removeLogs) {
        removeLogFiles(hostname);
      }
      if (deleteDatabase) {
        deleteDatabase(hostname);
      }
      switch (action) {
        case UPDATE:
          copyJarAndConfFiles(hostname);
          break;
        case RESTART:
          break;
      }
      // write the name-server-info
      writeNSFile(hostname);
      startServers(id, hostname);
      System.out.println("#### Node " + id + " running on " + hostname + " finished update ####");
    } else {
      killAllServers(hostname);
      System.out.println("#### Node " + id + " running on " + hostname + " has been stopped ####");
    }
  }

  private static final String CHANGETOINSTALLDIR
          = "# make current directory the directory this script is in\n"
          + "DIR=\"$( cd \"$( dirname \"${BASH_SOURCE[0]}\" )\" && pwd )\"\n"
          + "cd $DIR\n";

  /**
   * Starts an LNS, NS server on the remote host.
   *
   * @param id
   * @param hostname
   */
  private static void startServers(int id, String hostname) {
    System.out.println("Starting local name servers");
    File keyFile = getKeyFile();
    ExecuteBash.executeBashScriptNoSudo(userName, hostname, keyFile, buildInstallFilePath("runLNS.sh"),
            "#!/bin/bash\n"
            + CHANGETOINSTALLDIR
            + "if [ -f LNSlogfile ]; then\n"
            + "mv --backup=numbered LNSlogfile LNSlogfile.save\n"
            + "fi\n"
            + "nohup java -cp " + gnsFileName + " " + StartLNSClass + " "
            + "-id " + getLNSId(id)
            + " -configFile lns.conf "
            + "> LNSlogfile 2>&1 &");

    System.out.println("Starting name servers");
    ExecuteBash.executeBashScriptNoSudo(userName, hostname, keyFile, buildInstallFilePath("runNS.sh"),
            "#!/bin/bash\n"
            + CHANGETOINSTALLDIR
            + "if [ -f NSlogfile ]; then\n"
            + "mv --backup=numbered NSlogfile NSlogfile.save\n"
            + "fi\n"
            + "nohup java -cp " + gnsFileName + " " + StartNSClass + " "
            + " -id " + id
            + " -configFile ns.conf "
            + "> NSlogfile 2>&1 &");
    System.out.println("All servers started");
  }

  /**
   * Copies the JAR and configuration files to the remote host.
   *
   * @param id
   * @param hostname
   */
  private static void copyJarAndConfFiles(String hostname) {
    File keyFile = getKeyFile();
    System.out.println("Copying jar and conf files");
    RSync.upload(userName, hostname, keyFile, gnsJarFileLocation, buildInstallFilePath(gnsFileName));
    RSync.upload(userName, hostname, keyFile, lnsConfFileLocation, buildInstallFilePath(lnsConfFileName));
    RSync.upload(userName, hostname, keyFile, nsConfFileLocation, buildInstallFilePath(nsConfFileName));
  }

  /**
   * Runs the script file on the remote host.
   *
   * @param id
   * @param hostname
   */
  private static void executeScriptFile(int id, String hostname, String scriptFileLocation) {
    File keyFile = getKeyFile();
    System.out.println("Copying script file");
    // copy the file to remote host
    String remoteFile = Paths.get(scriptFileLocation).getFileName().toString();
    RSync.upload(userName, hostname, keyFile, scriptFileLocation, buildInstallFilePath(remoteFile));
    // make it executable
    SSHClient.exec(userName, hostname, keyFile, "chmod ugo+x" + " " + buildInstallFilePath(remoteFile));
    //execute it
    SSHClient.exec(userName, hostname, keyFile, "." + FILESEPARATOR + buildInstallFilePath(remoteFile));
  }

  private static void makeInstallDir(String hostname) {
    System.out.println("Creating install directory");
    if (installPath != null) {
      File keyFile = getKeyFile();
      SSHClient.exec(userName, hostname, keyFile, "mkdir -p " + installPath);
    }
  }

  //
  private static final String MongoRecordsClass = "edu.umass.cs.gns.database.MongoRecords";
  private static final String CassandraRecordsClass = "edu.umass.cs.gns.database.CassandraRecords";

  /**
   * Deletes the database on the remote host.
   *
   * @param id
   * @param hostname
   */
  private static void deleteDatabase(String hostname) {
    ExecuteBash.executeBashScriptNoSudo(userName, hostname, getKeyFile(), buildInstallFilePath("deleteDatabase.sh"),
            "#!/bin/bash\n"
            + CHANGETOINSTALLDIR
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
  private static void killAllServers(String hostname) {
    System.out.println("Killing GNS servers");
    ExecuteBash.executeBashScriptNoSudo(userName, hostname, getKeyFile(), buildInstallFilePath("killAllServers.sh"),
            "pkill -f \"java -cp GNS.jar\"");
    //"#!/bin/bash\nkillall java");
  }

  /**
   * Removes log files on the remote host.
   *
   * @param id
   * @param hostname
   */
  private static void removeLogFiles(String hostname) {
    System.out.println("Removing log files");
    ExecuteBash.executeBashScriptNoSudo(userName, hostname, getKeyFile(), buildInstallFilePath("removelogs.sh"),
            "#!/bin/bash\n"
            + CHANGETOINSTALLDIR
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
  private static void writeNSFile(String hostname) throws UnknownHostException {
    StringBuilder result = new StringBuilder();
    //HostID IsNS? IPAddress [StartingPort | - ] Ping-Latency Latitude Longitude
    // WRITE OUT NSs
    for (HostInfo info : hostTable.values()) {
      result.append(info.getId());
      result.append(" yes ");
      //result.append(info.getHostIP());
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
      //result.append(info.getHostIP());
      result.append(info.getHostname());
      result.append(" default ");
      result.append(" 0 ");
      result.append(info.getLocation() != null ? Format.formatLatLong(info.getLocation().getY()) : 0.0);
      result.append(" ");
      result.append(info.getLocation() != null ? Format.formatLatLong(info.getLocation().getX()) : 0.0);
      result.append(NEWLINE);
    }

    try {
      File temp = File.createTempFile("name-server-info", "");
      BufferedWriter bw = new BufferedWriter(new FileWriter(temp));
      bw.write(result.toString());
      bw.close();
      RSync.upload(userName, hostname, getKeyFile(), temp.getAbsolutePath(), buildInstallFilePath("name-server-info"));
    } catch (IOException e) {
      GNS.getLogger().severe("Unable to write temporary name-server-info file: " + e);
    }

  }

  // Probably unnecessary at this point.
  private static void updateNodeConfigAndSendOutServerInit() {
    GNSNodeConfig nodeConfig = new GNSNodeConfig();
    for (HostInfo info : hostTable.values()) {
      nodeConfig.addHostInfo(info.getId(), info.getHostname(), GNS.STARTINGPORT, 0, info.getLocation().getY(), info.getLocation().getX());
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
    File jarPath = getLocalJarPath();
    System.out.println("Jar path: " + jarPath);
    gnsJarFileLocation = jarPath.getPath();
    distFolderLocation = jarPath.getParent();
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
  private static File getLocalJarPath() {
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

  private static String buildInstallFilePath(String filename) {
    if (installPath == null) {
      return filename;
    } else {
      return installPath + FILESEPARATOR + filename;
    }
  }

  // COMMAND LINE STUFF
  private static HelpFormatter formatter = new HelpFormatter();
  private static Options commandLineOptions;

  private static CommandLine initializeOptions(String[] args) throws ParseException {
    Option help = new Option("help", "Prints Usage");
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
    Option scriptFile = OptionBuilder.withArgName("install script file").hasArg()
            .withDescription("specifies the location of a bash script file that will install MongoDB and Java 1.7")
            .create("scriptFile");
    Option stop = OptionBuilder.withArgName("runSet name").hasArg()
            .withDescription("stops GNS servers in a runset")
            .create("stop");

    commandLineOptions = new Options();
    commandLineOptions.addOption(update);
    commandLineOptions.addOption(restart);
    commandLineOptions.addOption(stop);
    commandLineOptions.addOption(removeLogs);
    commandLineOptions.addOption(deleteDatabase);
    commandLineOptions.addOption(dataStore);
    commandLineOptions.addOption(scriptFile);
    commandLineOptions.addOption(help);

    CommandLineParser parser = new GnuParser();
    return parser.parse(commandLineOptions, args);
  }

  private static void printUsage() {
    formatter.printHelp("java -cp GNS.jar edu.umass.cs.gns.installer.GNSInstaller <options>", commandLineOptions);
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
      String runsetStop = parser.getOptionValue("stop");
      String dataStoreName = parser.getOptionValue("datastore");
      boolean removeLogs = parser.hasOption("removeLogs");
      boolean deleteDatabase = parser.hasOption("deleteDatabase");
      String scriptFile = parser.getOptionValue("scriptFile");

      if (dataStoreName != null) {
        try {
          dataStoreType = DataStoreType.valueOf(dataStoreName);
        } catch (IllegalArgumentException e) {
          System.out.println("Unknown data store type " + dataStoreName + "; exiting.");
          System.exit(1);
        }
      }

      String configName = runsetUpdate != null ? runsetUpdate
              : runsetRestart != null ? runsetRestart
              : runsetStop != null ? runsetStop
              : null;

      System.out.println("Config name: " + configName);
      System.out.println("Current directory: " + System.getProperty("user.dir"));
      if (configName != null) {
        if (!loadConfig(configName)) {
          System.exit(1);
        }
      }

      if (!setupJarAndConfFilePaths()) {
        System.out.println("Can't locate needed config files. LNS conf: " + lnsConfFileLocation + " NS conf: " + nsConfFileLocation);
        System.exit(1);
      }

      SSHClient.setVerbose(true);
      RSync.setVerbose(true);

      if (runsetUpdate != null) {
        updateRunSet(runsetUpdate, InstallerAction.UPDATE, removeLogs, deleteDatabase, scriptFile);
      } else if (runsetRestart != null) {
        updateRunSet(runsetUpdate, InstallerAction.RESTART, removeLogs, deleteDatabase, scriptFile);
      } else if (runsetStop != null) {
        updateRunSet(runsetUpdate, InstallerAction.STOP, false, false, null);
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

    private final String hostname;
    private final int id;
    private final InstallerAction action;
    private final boolean removeLogs;
    private final boolean deleteDatabase;
    private final String scriptFile;

    public UpdateThread(String hostname, int id, InstallerAction action, boolean removeLogs, boolean deleteDatabase, String scriptFile) {
      this.hostname = hostname;
      this.id = id;
      this.action = action;
      this.removeLogs = removeLogs;
      this.deleteDatabase = deleteDatabase;
      this.scriptFile = scriptFile;
    }

    @Override
    public void run() {
      try {
        GNSInstallerV2.updateAndRunGNS(id, hostname, action, removeLogs, deleteDatabase, scriptFile);
      } catch (UnknownHostException e) {
        GNS.getLogger().info("Unknown hostname while updating " + hostname + ": " + e);
      }
    }
  }

}
