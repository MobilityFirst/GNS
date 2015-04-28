package edu.umass.cs.gns.installer;

import edu.umass.cs.gns.nodeconfig.HostFileLoader;
import edu.umass.cs.aws.networktools.ExecuteBash;
import edu.umass.cs.aws.networktools.RSync;
import edu.umass.cs.aws.networktools.SSHClient;
import edu.umass.cs.gns.database.DataStoreType;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nodeconfig.HostSpec;
import edu.umass.cs.gns.util.Format;
import java.io.File;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
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
 * Installs n instances of the GNS Jars on remote hosts and executes them.
 * More specifically this copies the GNS JAR and all the required config files
 * to the remote host then starts a Name Server and a Local Name server
 * on each host.
 *
 * Typical uses:
 *
 * First time install:
 * java -cp GNS.jar edu.umass.cs.gns.installer.GNSInstaller -scriptFile conf/ec2_mongo_java_install.bash -update ec2_dev_small
 *
 * Later updates:
 * java -cp GNS.jar edu.umass.cs.gns.installer.GNSInstaller -update ec2_dev_small
 *
 *
 * @author westy
 */
public class GNSInstaller {

  private static final String FILESEPARATOR = System.getProperty("file.separator");
  private static final String CONF_FOLDER = FILESEPARATOR + "conf";
  private static final String KEYHOME = System.getProperty("user.home") + FILESEPARATOR + ".ssh";
  public static final DataStoreType DEFAULT_DATA_STORE_TYPE = DataStoreType.MONGO;
  private static final String DEFAULT_USERNAME = "ec2-user";
  private static final String DEFAULT_KEYNAME = "id_rsa";
  private static final String DEFAULT_INSTALL_PATH = "gns";
  private static final String INSTALLER_CONFIG_FILENAME = "installer_config";
  private static final String LNS_CONF_FILENAME = "lns.conf";
  private static final String NS_CONF_FILENAME = "ns.conf";
  private static final String LNS_HOSTS_FILENAME = "lns_hosts.txt";
  private static final String NS_HOSTS_FILENAME = "ns_hosts.txt";
  private static final String JAVA_COMMAND = "java -ea -Xms1024M -cp ";

  /**
   * Stores information about the hosts we're using.
   * Contains info for both NS and LNS hosts. Could be split up onto one table for each.
   */
  private static ConcurrentHashMap<String, HostInfo> hostTable = new ConcurrentHashMap<String, HostInfo>();
  //
  private static DataStoreType dataStoreType = DEFAULT_DATA_STORE_TYPE;
  private static String hostType = "linux";
  private static String userName = DEFAULT_USERNAME;
  private static String keyFile = DEFAULT_KEYNAME;
  private static String installPath = DEFAULT_INSTALL_PATH;
  // calculated from the Jar location
  private static String distFolderPath;
  private static String gnsJarFileLocation;
  private static String confFolderPath;
  // these are mostly for convienence; could compute the when needed
  private static String nsConfFileLocation;
  private static String lnsConfFileLocation;
  private static String gnsJarFileName;
  private static String lnsConfFileName;
  private static String nsConfFileName;

  private static final String StartLNSClass = "edu.umass.cs.gns.newApp.localNameServer.NewLocalNameServer";
  private static final String StartNSClass = "edu.umass.cs.gns.newApp.AppReconfigurableNode";

  private static final String CHANGETOINSTALLDIR
          = "# make current directory the directory this script is in\n"
          + "DIR=\"$( cd \"$( dirname \"${BASH_SOURCE[0]}\" )\" && pwd )\"\n"
          + "cd $DIR\n";

  private static final String MongoRecordsClass = "edu.umass.cs.gns.database.MongoRecords";
  private static final String CassandraRecordsClass = "edu.umass.cs.gns.database.CassandraRecords";

  private static void loadConfig(String configName) {

    File configFile = fileSomewhere(configName + FILESEPARATOR + INSTALLER_CONFIG_FILENAME, confFolderPath);
    InstallConfig installConfig = new InstallConfig(configFile.toString());

    keyFile = installConfig.getKeyFile();
    System.out.println("Key File: " + keyFile);
    userName = installConfig.getUsername();
    System.out.println("User Name: " + userName);
    dataStoreType = installConfig.getDataStoreType();
    System.out.println("Data Store Type: " + dataStoreType);
    hostType = installConfig.getHostType();
    System.out.println("Host Type: " + hostType);
    installPath = installConfig.getInstallPath();
    if (installPath == null) {
      installPath = DEFAULT_INSTALL_PATH;
    }
    System.out.println("Install Path: " + installPath);
  }

  private static void loadHostsFiles(String configName) {
    List<HostSpec> nsHosts = null;

    File hostsFile = null;
    try {
      hostsFile = fileSomewhere(configName + FILESEPARATOR + NS_HOSTS_FILENAME, confFolderPath);
      nsHosts = HostFileLoader.loadHostFile(hostsFile.toString());
    } catch (Exception e) {
      // should not happen as we've already verified this above
      System.out.println("Problem loading the NS host file " + hostsFile + " : " + e);
      System.exit(1);
    }

    for (HostSpec spec : nsHosts) {
      String hostname = spec.getName();
      String id = (String) spec.getId();
      hostTable.put(hostname, new HostInfo(hostname, id, false, null));
    }

    List<HostSpec> lnsHosts = null;
    try {
      hostsFile = fileSomewhere(configName + FILESEPARATOR + LNS_HOSTS_FILENAME, confFolderPath);
      lnsHosts = HostFileLoader.loadHostFile(hostsFile.toString());
      // should not happen as we've already verified this above
    } catch (Exception e) {
      System.out.println("Problem loading the LNS host file " + hostsFile + e);
      System.exit(1);
    }
    // FIXME: BROKEN FOR IDLESS LNS HOSTS
    for (HostSpec spec : lnsHosts) {
      String hostname = spec.getName();
      //String id = spec.getId();
      HostInfo hostEntry = hostTable.get(hostname);
      if (hostEntry != null) {
        hostEntry.createLNS(true);
      } else {
        hostTable.put(hostname, new HostInfo(hostname, null, true, null));
      }
    }
  }

  /**
   * Copies the latest version of the JAR files to the all the hosts in the installation given by name and restarts all the servers.
   * Does this using a separate Thread for each host.
   *
   *
   * @param name
   * @param action
   * @param removeLogs
   * @param deleteDatabase
   * @param lnsHostsFile
   * @param nsHostsFile
   * @param scriptFile
   */
  public static void updateRunSet(String name, InstallerAction action, boolean removeLogs, boolean deleteDatabase,
          String lnsHostsFile, String nsHostsFile, String scriptFile) {
    ArrayList<Thread> threads = new ArrayList<Thread>();
    for (HostInfo info : hostTable.values()) {
      threads.add(new UpdateThread(info.getHostname(), info.getNsId(), info.isCreateLNS(), action, removeLogs, deleteDatabase,
              lnsHostsFile, nsHostsFile, scriptFile));
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
//    if (action != InstallerAction.STOP) {
//      updateNodeConfigAndSendOutServerInit();
//    }

    System.out.println("Finished " + name + " " + action.name() + " at " + Format.formatDateTimeOnly(new Date()));
  }

  public enum InstallerAction {

    UPDATE,
    RESTART,
    STOP,
  };

  /**
   * This is called to install and run the GNS on a single host. This is called concurrently in
   * one thread per each host.
   * This assumes a configuration where there is an LNS running at every node (in addition to the
   * NS).
   * Copies the JAR and conf files and optionally resets some other stuff depending on the
   * update action given.
   * The name-server-info file is created using all the IP address of all the hosts.
   * Then the various servers are started on the host.
   *
   * @param nsId
   * @param hostname
   * @param action
   * @param removeLogs
   * @param deleteDatabase
   * @param scriptFile
   * @throws java.net.UnknownHostException
   */
  public static void updateAndRunGNS(String nsId, boolean createLNS, String hostname, InstallerAction action, boolean removeLogs,
          boolean deleteDatabase, String lnsHostsFile, String nsHostsFile, String scriptFile) throws UnknownHostException {
    if (!action.equals(InstallerAction.STOP)) {
      System.out.println("**** NS " + nsId + " Create LNS " + createLNS + " running on " + hostname + " starting update ****");
      if (action == InstallerAction.UPDATE) {
        makeInstallDir(hostname);
      }
      killAllServers(hostname);
      if (scriptFile != null) {
        executeScriptFile(hostname, scriptFile);
      }
      if (removeLogs) {
        removeLogFiles(hostname);
      }
      if (deleteDatabase) {
        deleteDatabase(hostname);
      }
      switch (action) {
        case UPDATE:
          copyJarAndConfFiles(hostname, createLNS);
          copyHostsFiles(hostname, createLNS ? lnsHostsFile : null, nsHostsFile);
          break;
        case RESTART:
          break;
      }
      startServers(nsId, createLNS, hostname);
      System.out.println("#### NS " + nsId + " Create LNS " + createLNS + " running on " + hostname + " finished update ####");
    } else {
      killAllServers(hostname);
      System.out.println("#### NS " + nsId + " Create LNS " + createLNS + " running on " + hostname + " has been stopped ####");
    }
  }

  /**
   * Starts an LNS, NS server on the remote host.
   *
   * @param id
   * @param hostname
   */
  private static void startServers(String nsId, boolean createLNS, String hostname) {
    File keyFileName = getKeyFile();
    if (createLNS) {
      System.out.println("Starting local name servers");
      ExecuteBash.executeBashScriptNoSudo(userName, hostname, keyFileName, buildInstallFilePath("runLNS.sh"),
              "#!/bin/bash\n"
              + CHANGETOINSTALLDIR
              + "if [ -f LNSlogfile ]; then\n"
              + "mv --backup=numbered LNSlogfile LNSlogfile.save\n"
              + "fi\n"
              + "nohup " + JAVA_COMMAND + gnsJarFileName + " " + StartLNSClass + " "
              + hostname + " "
              + GNS.DEFAULT_LNS_TCP_PORT + " "
              // YES, THIS SHOULD BE NS_HOSTS_FILENAME, the LNS needs this
              + NS_HOSTS_FILENAME + " "
              // + " -configFile lns.conf "
              + " > LNSlogfile 2>&1 &");
    }
    if (nsId != null) {
      System.out.println("Starting name servers");
      ExecuteBash.executeBashScriptNoSudo(userName, hostname, keyFileName, buildInstallFilePath("runNS.sh"),
              "#!/bin/bash\n"
              + CHANGETOINSTALLDIR
              + "if [ -f NSlogfile ]; then\n"
              + "mv --backup=numbered NSlogfile NSlogfile.save\n"
              + "fi\n"
              + "nohup " + JAVA_COMMAND + gnsJarFileName + " " + StartNSClass + " "
              + nsId.toString() + " "
              + NS_HOSTS_FILENAME + " "
              //+ " -lnsfile " + LNS_HOSTS_FILENAME
              //+ " -configFile ns.conf "
              + " > NSlogfile 2>&1 &");
    }
    System.out.println("All servers started");
  }

  /**
   * Copies the JAR and configuration files to the remote host.
   *
   * @param id
   * @param hostname
   */
  private static void copyJarAndConfFiles(String hostname, boolean createLNS) {
    File keyFileName = getKeyFile();
    System.out.println("Copying jar and conf files");
    RSync.upload(userName, hostname, keyFileName, gnsJarFileLocation, buildInstallFilePath(gnsJarFileName));
    if (createLNS) {
      RSync.upload(userName, hostname, keyFileName, lnsConfFileLocation, buildInstallFilePath(lnsConfFileName));
    }
    RSync.upload(userName, hostname, keyFileName, nsConfFileLocation, buildInstallFilePath(nsConfFileName));
  }

  /**
   * Runs the script file on the remote host.
   *
   * @param id
   * @param hostname
   */
  private static void executeScriptFile(String hostname, String scriptFileLocation) {
    File keyFileName = getKeyFile();
    System.out.println("Copying script file");
    // copy the file to remote host
    String remoteFile = Paths.get(scriptFileLocation).getFileName().toString();
    RSync.upload(userName, hostname, keyFileName, scriptFileLocation, buildInstallFilePath(remoteFile));
    // make it executable
    SSHClient.exec(userName, hostname, keyFileName, "chmod ugo+x" + " " + buildInstallFilePath(remoteFile));
    //execute it
    SSHClient.exec(userName, hostname, keyFileName, "." + FILESEPARATOR + buildInstallFilePath(remoteFile));
  }

  private static void makeInstallDir(String hostname) {
    System.out.println("Creating install directory");
    if (installPath != null) {
      SSHClient.exec(userName, hostname, getKeyFile(), "mkdir -p " + installPath);
    }
  }

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
            + "java -cp " + gnsJarFileName + " " + MongoRecordsClass + " -clear");
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
            "pkill -f \"" + JAVA_COMMAND + gnsJarFileName + "\"");
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
            + "rm -rf derby.log\n"
            + "rm -rf paxos_logs\n"
            + "rm -rf reconfiguration_DB\n"
            + "rm -rf paxos_large_checkpoints\n"
            + "rm -rf paxoslog");
  }

  private static void copyHostsFiles(String hostname, String lnsHostsFile, String nsHostsFile) {
    File keyFileName = getKeyFile();
    System.out.println("Copying hosts files");
    RSync.upload(userName, hostname, keyFileName, nsHostsFile, buildInstallFilePath(NS_HOSTS_FILENAME));
    if (lnsHostsFile != null) {
      RSync.upload(userName, hostname, keyFileName, lnsHostsFile, buildInstallFilePath(LNS_HOSTS_FILENAME));
    }
  }

//  @SuppressWarnings("unchecked")
//  // Probably unnecessary at this point.
//  private static void updateNodeConfigAndSendOutServerInit() {
//    GNSNodeConfig nodeConfig = new GNSNodeConfig();
//    Set< String> ids = new HashSet<>();
//    for (HostInfo info : hostTable.values()) {
//      if (info.getNsId() != null) {
//        nodeConfig.addHostInfo(info.getNsId(), info.getHostname(), GNS.STARTINGPORT, 0, info.getLocation().getY(), info.getLocation().getX());
//        ids.add(info.getNsId());
//      }
//    }
//    // now we send out packets telling all the hosts where to send their status updates
//    StatusListener.sendOutServerInitPackets(nodeConfig, ids);
//  }
  /**
   * Figures out the locations of the JAR and conf files.
   *
   * @return true if it found them
   */
  private static void setupJarPath() {
    File jarPath = getLocalJarPath();
    System.out.println("Jar path: " + jarPath);
    gnsJarFileLocation = jarPath.getPath();
    distFolderPath = jarPath.getParent();
    confFolderPath = distFolderPath + CONF_FOLDER;
    System.out.println("Conf folder path: " + confFolderPath);
    gnsJarFileName = new File(gnsJarFileLocation).getName();
  }

  // checks for an absolute or relative path, then checks for a path in "blessed" location.
  private static boolean fileExistsSomewhere(String filename, String fileInConfigFolder) {
    return fileSomewhere(filename, fileInConfigFolder) != null;
  }

  private static File fileSomewhere(String filename, String blessedPath) {

    File file = new File(filename);
    if (file.exists()) {
      return file;
    }
    file = new File(blessedPath + FILESEPARATOR + filename);
    if (file.exists()) {
      return file;
    }
    System.out.println("Failed to find: " + filename);
    System.out.println("Also failed to find: " + blessedPath + FILESEPARATOR + filename);
    return null;
  }

  private static boolean checkAndSetConfFilePaths(String configNameOrFolder) {

    // first check for a least a config folder
    if (!fileExistsSomewhere(configNameOrFolder, confFolderPath)) {
      System.out.println("Config folder " + configNameOrFolder + " not found... exiting. ");
      System.exit(1);
    }

    if (!fileExistsSomewhere(configNameOrFolder + FILESEPARATOR + INSTALLER_CONFIG_FILENAME, confFolderPath)) {
      System.out.println("Config folder " + configNameOrFolder + " missing file " + INSTALLER_CONFIG_FILENAME);
    }
    if (!fileExistsSomewhere(configNameOrFolder + FILESEPARATOR + LNS_CONF_FILENAME, confFolderPath)) {
      System.out.println("Config folder " + configNameOrFolder + " missing file " + LNS_CONF_FILENAME);
    }
    if (!fileExistsSomewhere(configNameOrFolder + FILESEPARATOR + NS_CONF_FILENAME, confFolderPath)) {
      System.out.println("Config folder " + configNameOrFolder + " missing file " + NS_CONF_FILENAME);
    }
    if (!fileExistsSomewhere(configNameOrFolder + FILESEPARATOR + LNS_HOSTS_FILENAME, confFolderPath)) {
      System.out.println("Config folder " + configNameOrFolder + " missing file " + LNS_HOSTS_FILENAME);
    }
    if (!fileExistsSomewhere(configNameOrFolder + FILESEPARATOR + NS_HOSTS_FILENAME, confFolderPath)) {
      System.out.println("Config folder " + configNameOrFolder + " missing file " + NS_HOSTS_FILENAME);
    }
    lnsConfFileLocation = fileSomewhere(configNameOrFolder + FILESEPARATOR + LNS_CONF_FILENAME, confFolderPath).toString();
    nsConfFileLocation = fileSomewhere(configNameOrFolder + FILESEPARATOR + NS_CONF_FILENAME, confFolderPath).toString();
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
    // check using full path
    return new File(keyFile).exists() ? new File(keyFile)
            : // also check in blessed location
            new File(KEYHOME + FILESEPARATOR + keyFile).exists() ? new File(KEYHOME + FILESEPARATOR + keyFile) : null;

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
    Option update = OptionBuilder.withArgName("installation name").hasArg()
            .withDescription("updates GNS files and restarts servers in a installation")
            .create("update");
    Option restart = OptionBuilder.withArgName("installation name").hasArg()
            .withDescription("restarts GNS servers in a installation")
            .create("restart");
    Option removeLogs = new Option("removeLogs", "remove paxos and Logger log files (use with -restart or -update)");
    Option deleteDatabase = new Option("deleteDatabase", "delete the databases in a installation (use with -restart or -update)");
    Option dataStore = OptionBuilder.withArgName("data store type").hasArg()
            .withDescription("data store type")
            .create("datastore");
    Option scriptFile = OptionBuilder.withArgName("install script file").hasArg()
            .withDescription("specifies the location of a bash script file that will install MongoDB and Java 1.7")
            .create("scriptFile");
    Option stop = OptionBuilder.withArgName("installation name").hasArg()
            .withDescription("stops GNS servers in a installation")
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
      if (parser.hasOption("help") || args.length == 0) {
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

      setupJarPath();
      if (!checkAndSetConfFilePaths(configName)) {
        System.exit(1);
      }

      if (getKeyFile() == null) {
        System.out.println("Can't find keyfile: " + keyFile + "; exiting.");
        System.exit(1);
      }

      loadConfig(configName);
      loadHostsFiles(configName);

      String lnsHostFile = fileSomewhere(configName + FILESEPARATOR + LNS_HOSTS_FILENAME, confFolderPath).toString();
      String nsHostFile = fileSomewhere(configName + FILESEPARATOR + NS_HOSTS_FILENAME, confFolderPath).toString();

      SSHClient.setVerbose(true);
      RSync.setVerbose(true);

      if (runsetUpdate != null) {
        updateRunSet(runsetUpdate, InstallerAction.UPDATE, removeLogs, deleteDatabase, lnsHostFile, nsHostFile, scriptFile);
      } else if (runsetRestart != null) {
        updateRunSet(runsetRestart, InstallerAction.RESTART, removeLogs, deleteDatabase, lnsHostFile, nsHostFile, scriptFile);
      } else if (runsetStop != null) {
        updateRunSet(runsetStop, InstallerAction.STOP, false, false, null, null, null);
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
    private final String nsId;
    private final boolean createLNS;
    private final InstallerAction action;
    private final boolean removeLogs;
    private final boolean deleteDatabase;
    private final String lnsHostsFile;
    private final String nsHostsFile;
    private final String scriptFile;

    public UpdateThread(String hostname, String nsId, boolean createLNS, InstallerAction action, boolean removeLogs, boolean deleteDatabase,
            String lnsHostsFile, String nsHostsFile, String scriptFile) {
      this.hostname = hostname;
      this.nsId = nsId;
      this.createLNS = createLNS;
      this.action = action;
      this.removeLogs = removeLogs;
      this.deleteDatabase = deleteDatabase;
      this.scriptFile = scriptFile;
      this.lnsHostsFile = lnsHostsFile;
      this.nsHostsFile = nsHostsFile;

    }

    @Override
    public void run() {
      try {
        GNSInstaller.updateAndRunGNS(nsId, createLNS, hostname, action, removeLogs, deleteDatabase, lnsHostsFile, nsHostsFile, scriptFile);
      } catch (UnknownHostException e) {
        GNS.getLogger().info("Unknown hostname while updating " + hostname + ": " + e);
      }
    }
  }

}
