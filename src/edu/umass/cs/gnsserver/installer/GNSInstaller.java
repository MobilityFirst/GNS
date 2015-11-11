/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *  Initial developer(s): Westy
 *
 */
package edu.umass.cs.gnsserver.installer;

import edu.umass.cs.gnsserver.nodeconfig.HostFileLoader;
import edu.umass.cs.aws.networktools.ExecuteBash;
import edu.umass.cs.aws.networktools.RSync;
import edu.umass.cs.aws.networktools.SSHClient;
import edu.umass.cs.gnsserver.main.GNS;
import edu.umass.cs.gnsserver.nodeconfig.HostSpec;
import edu.umass.cs.gnscommon.utils.Format;
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
 * java -cp GNS.jar edu.umass.cs.gnsserver.installer.GNSInstaller -scriptFile conf/ec2_mongo_java_install.bash -update kittens.name
 *
 * Later updates:
 * java -cp GNS.jar edu.umass.cs.gnsserver.installer.GNSInstaller -update kittens.name
 *
 *
 * @author westy
 */
public class GNSInstaller {

  private static final String FILESEPARATOR = System.getProperty("file.separator");
  private static final String CONF_FOLDER = FILESEPARATOR + "conf";
  private static final String KEYHOME = System.getProperty("user.home") + FILESEPARATOR + ".ssh";

  /**
   * The default datastore type.
   */
  public static final DataStoreType DEFAULT_DATA_STORE_TYPE = DataStoreType.MONGO;
  private static final String DEFAULT_USERNAME = "ec2-user";
  private static final String DEFAULT_KEYNAME = "id_rsa";
  private static final String DEFAULT_INSTALL_PATH = "gns";
  private static final String INSTALLER_CONFIG_FILENAME = "installer_config";
  private static final String LNS_PROPERTIES_FILENAME = "lns.properties";
  private static final String NS_PROPERTIES_FILENAME = "ns.properties";
  private static final String PAXOS_PROPERTIES_FILENAME = "gigapaxos.gnsApp.properties";
  private static final String LNS_HOSTS_FILENAME = "lns_hosts.txt";
  private static final String NS_HOSTS_FILENAME = "ns_hosts.txt";
  private static final String DEFAULT_JAVA_COMMAND = "java -ea -Xms1024M";
  private static final String DEFAULT_JAVA_COMMAND_FOR_LNS = "java -ea -Xms512M";
  private static final String KEYSTORE_FOLDER_NAME = "keyStore";
  private static final String TRUSTSTORE_FOLDER_NAME = "trustStore";
  private static final String TRUST_STORE_OPTION = "-Djavax.net.ssl.trustStorePassword=qwerty -Djavax.net.ssl.trustStore=conf/trustStore/node100.jks";
  private static final String KEY_STORE_OPTION = "-Djavax.net.ssl.keyStorePassword=qwerty -Djavax.net.ssl.keyStore=conf/keyStore/node100.jks";
  private static final String SSL_DEBUG = "-Djavax.net.debug=ssl";
// should make this a config parameter
  //private static final String JAVA_COMMAND = "java -ea";

  /**
   * Stores information about the hosts we're using.
   * Contains info for both NS and LNS hosts. Could be split up onto one table for each.
   */
  private static final ConcurrentHashMap<String, HostInfo> hostTable = new ConcurrentHashMap<String, HostInfo>();
  //
  private static DataStoreType dataStoreType = DEFAULT_DATA_STORE_TYPE;
  private static String hostType = "linux";
  private static String userName = DEFAULT_USERNAME;
  private static String keyFile = DEFAULT_KEYNAME;
  private static String installPath = DEFAULT_INSTALL_PATH;
  private static String javaCommand = DEFAULT_JAVA_COMMAND;
  private static String javaCommandForLNS = DEFAULT_JAVA_COMMAND_FOR_LNS; // this one isn't changed by config
  // calculated from the Jar location
  private static String distFolderPath;
  private static String gnsJarFileLocation;
  private static String confFolderPath;
  // these are mostly for convienence; could compute them when needed
  private static String gnsJarFileName;
  private static String nsConfFileLocation;
  //private static String ccpConfFileLocation;
  private static String lnsConfFileLocation;
  private static String paxosConfFileLocation;
  private static String nsConfFileName;
  private static String lnsConfFileName;
  private static String paxosConfFileName;

  private static final String StartLNSClass = "edu.umass.cs.gnsserver.localnameserver.LocalNameServer";
  private static final String StartNSClass = "edu.umass.cs.gnsserver.gnsApp.AppReconfigurableNode";
  private static final String StartNoopClass = "edu.umass.cs.gnsserver.gnsApp.noopTest.DistributedNoopReconfigurableNode";

  private static final String CHANGETOINSTALLDIR
          = "# make current directory the directory this script is in\n"
          + "DIR=\"$( cd \"$( dirname \"${BASH_SOURCE[0]}\" )\" && pwd )\"\n"
          + "cd $DIR\n";

  private static final String MongoRecordsClass = "edu.umass.cs.gnsserver.database.MongoRecords";
  private static final String CassandraRecordsClass = "edu.umass.cs.gnsserver.database.CassandraRecords";

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
    //
    javaCommand = installConfig.getJavaCommand();
    if (javaCommand == null) {
      javaCommand = DEFAULT_JAVA_COMMAND;
    }
    System.out.println("Java Command: " + javaCommand);
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
        hostEntry.setCreateLNS(true);
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
   * @param runAsRoot
   * @param noopTest
   */
  public static void updateRunSet(String name, InstallerAction action, boolean removeLogs, boolean deleteDatabase,
          String lnsHostsFile, String nsHostsFile, String scriptFile, boolean runAsRoot, boolean noopTest) {
    ArrayList<Thread> threads = new ArrayList<>();
    for (HostInfo info : hostTable.values()) {
      threads.add(new UpdateThread(info.getHostname(), info.getNsId(),
              noopTest ? false : info.isCreateLNS(),
              action, removeLogs, deleteDatabase,
              lnsHostsFile, nsHostsFile, scriptFile, runAsRoot, noopTest));
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

  /**
   * What action to perform on the servers.
   */
  public enum InstallerAction {

    /**
     * Makes the installer kill the servers, update all the relevant files on the remote hosts and restart.
     */
    UPDATE,
    /**
     * Makes the installer just kill and restart all the servers.
     */
    RESTART,
    /**
     * Makes the installer kill the servers.
     */
    STOP,
  };

  /**
   * This is called to install and run the GNS on a single host. This is called concurrently in
   * one thread per each host. LNSs will be run on hosts according to the contents of the lns hosts
   * file.
   * Copies the JAR and conf files and optionally resets some other stuff depending on the
   * update action given.
   * Then the various servers are started on the host.
   *
   * @param nsId
   * @param createLNS
   * @param hostname
   * @param action
   * @param removeLogs
   * @param deleteDatabase
   * @param scriptFile
   * @param lnsHostsFile
   * @param nsHostsFile
   * @param runAsRoot
   * @param noopTest
   * @throws java.net.UnknownHostException
   */
  public static void updateAndRunGNS(String nsId, boolean createLNS, String hostname, InstallerAction action,
          boolean removeLogs, boolean deleteDatabase,
          String lnsHostsFile, String nsHostsFile, String scriptFile, boolean runAsRoot,
          boolean noopTest) throws UnknownHostException {
    if (!action.equals(InstallerAction.STOP)) {
      if (!noopTest) {
        System.out.println("**** NS " + nsId + " Create LNS " + createLNS + " running on " + hostname + " starting update ****");
      } else {
        System.out.println("#### Noop test " + nsId + " on " + hostname + " starting update ****");
      }
      if (action == InstallerAction.UPDATE) {
        makeInstallDir(hostname);
      }
      killAllServers(hostname, runAsRoot);
      if (scriptFile != null) {
        executeScriptFile(hostname, scriptFile);
      }
      if (removeLogs) {
        removeLogFiles(hostname, runAsRoot);
      }
      if (deleteDatabase) {
        deleteDatabase(hostname);
      }
      switch (action) {
        case UPDATE:
          makeConfAndcopyJarAndConfFiles(hostname, createLNS, noopTest);
          copyHostsFiles(hostname, createLNS ? lnsHostsFile : null, nsHostsFile);
          copySSLFiles(hostname);
          break;
        case RESTART:
          break;
      }
      if (!noopTest) {
        startServers(nsId, createLNS, hostname, runAsRoot);
        System.out.println("#### NS " + nsId + " Create LNS " + createLNS + " running on " + hostname + " finished update ####");
      } else {
        startNoopServers(nsId, hostname, runAsRoot);
        System.out.println("#### Noop test " + nsId + " on " + hostname + " finished update ####");
      }
    } else {
      killAllServers(hostname, runAsRoot);
      if (removeLogs) {
        removeLogFiles(hostname, runAsRoot);
      }
      if (deleteDatabase) {
        deleteDatabase(hostname);
      }
      System.out.println("#### NS " + nsId + " Create LNS " + createLNS + " running on " + hostname + " has been stopped ####");
    }
  }

  /**
   * Starts a pair of active replica / reconfigurator on each host in the ns hosts file
   * plus lns servers on each host in the lns hosts file.
   *
   * @param id
   * @param hostname
   */
  private static void startServers(String nsId, boolean createLNS, String hostname, boolean runAsRoot) {
    File keyFileName = getKeyFile();
    if (createLNS) {
      System.out.println("Starting local name servers");
      ExecuteBash.executeBashScriptNoSudo(userName, hostname, keyFileName, buildInstallFilePath("runLNS.sh"),
              "#!/bin/bash\n"
              + CHANGETOINSTALLDIR
              + "if [ -f LNSlogfile ]; then\n"
              + "mv --backup=numbered LNSlogfile LNSlogfile.save\n"
              + "fi\n"
              //+ ((runAsRoot) ? "sudo " : "")
              + "nohup " + javaCommandForLNS
              + " -cp " + gnsJarFileName
              //+ " " + SSL_DEBUG
              + " " + TRUST_STORE_OPTION
              + " " + KEY_STORE_OPTION
              + " " + StartLNSClass + " "
              // YES, THIS SHOULD BE NS_HOSTS_FILENAME, the LNS needs this
              + "-nsfile "
              + "conf" + FILESEPARATOR + NS_HOSTS_FILENAME + " "
              + "-configFile "
              + "conf" + FILESEPARATOR + LNS_PROPERTIES_FILENAME + " "
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
              + ((runAsRoot) ? "sudo " : "")
              + "nohup " + javaCommand
              + " -DgigapaxosConfig=" + "conf" + FILESEPARATOR + PAXOS_PROPERTIES_FILENAME + " "
              + " -cp " + gnsJarFileName
              //+ " " + SSL_DEBUG
              + " " + TRUST_STORE_OPTION
              + " " + KEY_STORE_OPTION
              + " " + StartNSClass + " "
              + "-id "
              + nsId.toString() + " "
              + "-nsfile "
              + "conf" + FILESEPARATOR + NS_HOSTS_FILENAME + " "
              + "-configFile "
              + "conf" + FILESEPARATOR + NS_PROPERTIES_FILENAME + " "
              //+ " -demandProfileClass edu.umass.cs.gnsserver.gnsApp.NullDemandProfile "
              + " > NSlogfile 2>&1 &");
    }
    System.out.println("All servers started");
  }

  /**
   * Starts a noop test server.
   *
   * @param nsId
   * @param hostname
   * @param runAsRoot
   */
  private static void startNoopServers(String nsId, String hostname, boolean runAsRoot) {
    File keyFileName = getKeyFile();
    if (nsId != null) {
      System.out.println("Starting noop server");
      ExecuteBash.executeBashScriptNoSudo(userName, hostname, keyFileName, buildInstallFilePath("runNoop.sh"),
              "#!/bin/bash\n"
              + CHANGETOINSTALLDIR
              + "if [ -f Nooplogfile ]; then\n"
              + "mv --backup=numbered Nooplogfile Nooplogfile.save\n"
              + "fi\n"
              + ((runAsRoot) ? "sudo " : "")
              + "nohup " + javaCommand + " -cp " + gnsJarFileName + " " + StartNoopClass + " "
              + nsId.toString() + " "
              + NS_HOSTS_FILENAME + " "
              + " > Nooplogfile 2>&1 &");
    }
    System.out.println("Noop server started");
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
  private static void killAllServers(String hostname, boolean runAsRoot) {
    System.out.println("Killing GNS servers");
    ExecuteBash.executeBashScriptNoSudo(userName, hostname, getKeyFile(),
            //runAsRoot,
            buildInstallFilePath("killAllServers.sh"),
            ((runAsRoot) ? "sudo " : "")
            + "pkill -f \"" + gnsJarFileName + "\""
    );
    //"#!/bin/bash\nkillall java");
  }

  /**
   * Removes log files on the remote host.
   *
   * @param id
   * @param hostname
   */
  private static void removeLogFiles(String hostname, boolean runAsRoot) {
    System.out.println("Removing log files");
    ExecuteBash.executeBashScriptNoSudo(userName, hostname, getKeyFile(),
            //runAsRoot,
            buildInstallFilePath("removelogs.sh"),
            "#!/bin/bash\n"
            + CHANGETOINSTALLDIR
            + ((runAsRoot) ? "sudo " : "")
            + "rm NSlogfile*\n"
            + ((runAsRoot) ? "sudo " : "")
            + "rm Nooplogfile*\n"
            + ((runAsRoot) ? "sudo " : "")
            + "rm LNSlogfile*\n"
            + ((runAsRoot) ? "sudo " : "")
            + "rm -rf log\n"
            + ((runAsRoot) ? "sudo " : "")
            + "rm -rf derby.log\n"
            + ((runAsRoot) ? "sudo " : "")
            + "rm -rf paxos_logs\n"
            + ((runAsRoot) ? "sudo " : "")
            + "rm -rf reconfiguration_DB\n"
            + ((runAsRoot) ? "sudo " : "")
            + "rm -rf paxos_large_checkpoints\n"
            + ((runAsRoot) ? "sudo " : "")
            + "rm -rf paxoslog");
  }

  private static void copyHostsFiles(String hostname, String lnsHostsFile, String nsHostsFile) {
    File keyFileName = getKeyFile();
    System.out.println("Copying hosts files");
    RSync.upload(userName, hostname, keyFileName, nsHostsFile,
            buildInstallFilePath("conf" + FILESEPARATOR + NS_HOSTS_FILENAME));
    if (lnsHostsFile != null) {
      RSync.upload(userName, hostname, keyFileName, lnsHostsFile,
              buildInstallFilePath("conf" + FILESEPARATOR + LNS_HOSTS_FILENAME));
    }
  }

  /**
   * Copies the JAR and configuration files to the remote host.
   *
   * @param id
   * @param hostname
   */
  private static void makeConfAndcopyJarAndConfFiles(String hostname, boolean createLNS, boolean noopTest) {
    if (installPath != null) {
      System.out.println("Creating conf, keystore and truststore directories");
      SSHClient.exec(userName, hostname, getKeyFile(), "mkdir -p " + installPath + CONF_FOLDER);

      SSHClient.exec(userName, hostname, getKeyFile(), "mkdir -p " + installPath + CONF_FOLDER + FILESEPARATOR + KEYSTORE_FOLDER_NAME);
      SSHClient.exec(userName, hostname, getKeyFile(), "mkdir -p " + installPath + CONF_FOLDER + FILESEPARATOR + TRUSTSTORE_FOLDER_NAME);

//      SSHClient.exec(userName, hostname, getKeyFile(), "rm " + installPath + FILESEPARATOR + "*.txt");
//      SSHClient.exec(userName, hostname, getKeyFile(), "rm " + installPath + FILESEPARATOR + "*.properties");
      File keyFileName = getKeyFile();
      System.out.println("Copying jar and conf files");
      RSync.upload(userName, hostname, keyFileName, gnsJarFileLocation, buildInstallFilePath(gnsJarFileName));
      if (createLNS && !noopTest) {
        RSync.upload(userName, hostname, keyFileName, lnsConfFileLocation,
                buildInstallFilePath("conf" + FILESEPARATOR + lnsConfFileName));
      }
      if (!noopTest) {
        RSync.upload(userName, hostname, keyFileName, nsConfFileLocation,
                buildInstallFilePath("conf" + FILESEPARATOR + nsConfFileName));
      }
      if (!noopTest) {
        RSync.upload(userName, hostname, keyFileName, paxosConfFileLocation,
                buildInstallFilePath("conf" + FILESEPARATOR + paxosConfFileName));
      }
    }
  }

  private static void copySSLFiles(String hostname) {
    File keyFileName = getKeyFile();
    System.out.println("Copying SSL files");
    RSync.upload(userName, hostname, keyFileName,
            confFolderPath + FILESEPARATOR + KEYSTORE_FOLDER_NAME + FILESEPARATOR + "node100.jks",
            buildInstallFilePath("conf" + FILESEPARATOR + KEYSTORE_FOLDER_NAME + FILESEPARATOR + "node100.jks"));
    RSync.upload(userName, hostname, keyFileName,
            confFolderPath + FILESEPARATOR + KEYSTORE_FOLDER_NAME + FILESEPARATOR + "node100.cer",
            buildInstallFilePath("conf" + FILESEPARATOR + KEYSTORE_FOLDER_NAME + FILESEPARATOR + "node100.cer"));
    RSync.upload(userName, hostname, keyFileName,
            confFolderPath + FILESEPARATOR + TRUSTSTORE_FOLDER_NAME + FILESEPARATOR + "node100.jks",
            buildInstallFilePath("conf" + FILESEPARATOR + TRUSTSTORE_FOLDER_NAME + FILESEPARATOR + "node100.jks"));
    RSync.upload(userName, hostname, keyFileName,
            confFolderPath + FILESEPARATOR + TRUSTSTORE_FOLDER_NAME + FILESEPARATOR + "node100.cer",
            buildInstallFilePath("conf" + FILESEPARATOR + TRUSTSTORE_FOLDER_NAME + FILESEPARATOR + "node100.cer"));
  }

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
    if (!fileExistsSomewhere(configNameOrFolder + FILESEPARATOR + LNS_PROPERTIES_FILENAME, confFolderPath)) {
      System.out.println("Config folder " + configNameOrFolder + " missing file " + LNS_PROPERTIES_FILENAME);
    }
    if (!fileExistsSomewhere(configNameOrFolder + FILESEPARATOR + NS_PROPERTIES_FILENAME, confFolderPath)) {
      System.out.println("Config folder " + configNameOrFolder + " missing file " + NS_PROPERTIES_FILENAME);
    }
    if (!fileExistsSomewhere(configNameOrFolder + FILESEPARATOR + PAXOS_PROPERTIES_FILENAME, confFolderPath)) {
      System.out.println("Config folder " + configNameOrFolder + " missing file " + PAXOS_PROPERTIES_FILENAME);
    }
    if (!fileExistsSomewhere(configNameOrFolder + FILESEPARATOR + LNS_HOSTS_FILENAME, confFolderPath)) {
      System.out.println("Config folder " + configNameOrFolder + " missing file " + LNS_HOSTS_FILENAME);
    }
    if (!fileExistsSomewhere(configNameOrFolder + FILESEPARATOR + NS_HOSTS_FILENAME, confFolderPath)) {
      System.out.println("Config folder " + configNameOrFolder + " missing file " + NS_HOSTS_FILENAME);
    }
    lnsConfFileLocation = fileSomewhere(configNameOrFolder + FILESEPARATOR + 
            LNS_PROPERTIES_FILENAME, confFolderPath).toString();
    nsConfFileLocation = fileSomewhere(configNameOrFolder + FILESEPARATOR + 
            NS_PROPERTIES_FILENAME, confFolderPath).toString();
    paxosConfFileLocation = fileSomewhere(configNameOrFolder + FILESEPARATOR + 
            PAXOS_PROPERTIES_FILENAME, confFolderPath).toString();
    lnsConfFileName = new File(lnsConfFileLocation).getName();
    nsConfFileName = new File(nsConfFileLocation).getName();
    paxosConfFileName = new File(paxosConfFileLocation).getName();

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
   * @return a File
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
    Option root = new Option("root", "run the installation as root");
    Option noopTest = new Option("noopTest", "starts noop test servers instead of GNS APP servers");

    commandLineOptions = new Options();
    commandLineOptions.addOption(update);
    commandLineOptions.addOption(restart);
    commandLineOptions.addOption(stop);
    commandLineOptions.addOption(removeLogs);
    commandLineOptions.addOption(deleteDatabase);
    commandLineOptions.addOption(dataStore);
    commandLineOptions.addOption(scriptFile);
    commandLineOptions.addOption(root);
    commandLineOptions.addOption(noopTest);
    commandLineOptions.addOption(help);

    CommandLineParser parser = new GnuParser();
    return parser.parse(commandLineOptions, args);
  }

  private static void printUsage() {
    formatter.printHelp("java -cp GNS.jar edu.umass.cs.gnsserver.installer.GNSInstaller <options>", commandLineOptions);
  }

  /**
   * The main routine.
   *
   * @param args
   */
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
      boolean runAsRoot = parser.hasOption("root");
      boolean noopTest = parser.hasOption("noopTest");

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
        updateRunSet(runsetUpdate, InstallerAction.UPDATE, removeLogs, deleteDatabase,
                lnsHostFile, nsHostFile, scriptFile, runAsRoot, noopTest);
      } else if (runsetRestart != null) {
        updateRunSet(runsetRestart, InstallerAction.RESTART, removeLogs, deleteDatabase,
                lnsHostFile, nsHostFile, scriptFile, runAsRoot, noopTest);
      } else if (runsetStop != null) {
        updateRunSet(runsetStop, InstallerAction.STOP, removeLogs, deleteDatabase,
                null, null, null, runAsRoot, noopTest);
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
  private static class UpdateThread extends Thread {

    private final String hostname;
    private final String nsId;
    private final boolean createLNS;
    private final InstallerAction action;
    private final boolean removeLogs;
    private final boolean deleteDatabase;
    private final String lnsHostsFile;
    private final String nsHostsFile;
    private final String scriptFile;
    private final boolean runAsRoot;
    private final boolean noopTest;

    public UpdateThread(String hostname, String nsId, boolean createLNS, InstallerAction action, boolean removeLogs, boolean deleteDatabase,
            String lnsHostsFile, String nsHostsFile, String scriptFile, boolean runAsRoot, boolean noopTest) {
      this.hostname = hostname;
      this.nsId = nsId;
      this.createLNS = createLNS;
      this.action = action;
      this.removeLogs = removeLogs;
      this.deleteDatabase = deleteDatabase;
      this.scriptFile = scriptFile;
      this.lnsHostsFile = lnsHostsFile;
      this.nsHostsFile = nsHostsFile;
      this.runAsRoot = runAsRoot;
      this.noopTest = noopTest;
    }

    @Override
    public void run() {
      try {
        GNSInstaller.updateAndRunGNS(nsId, createLNS, hostname, action, removeLogs, deleteDatabase,
                lnsHostsFile, nsHostsFile, scriptFile, runAsRoot, noopTest);
      } catch (UnknownHostException e) {
        GNS.getLogger().info("Unknown hostname while updating " + hostname + ": " + e);
      }
    }
  }

}
