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

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Tag;
import edu.umass.cs.aws.support.AMIRecord;
import edu.umass.cs.aws.support.AMIRecordType;
import edu.umass.cs.aws.support.AWSEC2;
import edu.umass.cs.aws.support.InstanceStateRecord;
import edu.umass.cs.aws.support.RegionRecord;
import edu.umass.cs.gnsserver.main.GNS;
import edu.umass.cs.gnsserver.utils.GEOLocator;
import edu.umass.cs.gnscommon.utils.ThreadUtils;
import java.awt.geom.Point2D;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import javax.swing.JDialog;
import javax.swing.JOptionPane;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * Starts a set of EC2 instances.
 * Reads from a XML configuration file whose name is specified on the command line.
 *
 * Typical use:
 *
 * java -cp GNS.jar edu.umass.cs.gnsserver.installer.EC2Runner -create dev
 *
 * @author westy
 */
public class EC2Runner {

  private static final String FILESEPARATOR = System.getProperty("file.separator");
  private static final String CREDENTIALSFILE = System.getProperty("user.home") + FILESEPARATOR + "AwsCredentials.properties";
  private static final DataStoreType DEFAULT_DATA_STORE_TYPE = DataStoreType.MONGO;
  private static final AMIRecordType DEFAULT_AMI_RECORD_TYPE = AMIRecordType.Amazon_Linux_AMI_2013_03_1;
  private static final String DEFAULT_EC2_USERNAME = "ec2-user";
  /**
   * Contains information read from config file on what hosts we are trying to start.
   */
  private static List<EC2RegionSpec> regionsList = new ArrayList<EC2RegionSpec>();
  /**
   * Stores information about instances that have started.
   */
  private static ConcurrentHashMap<String, HostInfo> hostTable = new ConcurrentHashMap<String, HostInfo>();
  //
  private static final int STARTINGNODENUMBER = 0;
  private static ConcurrentHashMap<String, String> hostsThatDidNotStart = new ConcurrentHashMap<String, String>();
  private static DataStoreType dataStoreType = DEFAULT_DATA_STORE_TYPE;
  private static AMIRecordType amiRecordType = DEFAULT_AMI_RECORD_TYPE;
  private static String ec2UserName = DEFAULT_EC2_USERNAME;
  private static String configName = null;

  private static void loadConfig(String configName) {
    EC2ConfigParser parser = new EC2ConfigParser(configName);
    for (EC2RegionSpec spec : parser.getRegions()) {
      //System.out.println(spec.toString());
      regionsList.add(new EC2RegionSpec(spec.getRegion(), spec.getCount(), spec.getIp()));
    }
    ec2UserName = parser.getEc2username();
    dataStoreType = parser.getDataStoreType();
    amiRecordType = parser.getAmiRecordType();
  }

  /**
   * Starts a set of EC2 hosts running GNS that we call a runset.
   *
   * @param runSetName
   */
  public static void createRunSetMulti(String runSetName) {
    int timeout = AWSEC2.DEFAULTREACHABILITYWAITTIME;
    System.out.println("EC2 User Name: " + ec2UserName);
    System.out.println("AMI Name: " + amiRecordType.toString());
    System.out.println("Datastore: " + dataStoreType.toString());
    //preferences.put(RUNSETNAME, runSetName); // store the last one
    startAllMonitoringAndGUIProcesses();
    attachShutDownHook(runSetName);
    ArrayList<Thread> threads = new ArrayList<Thread>();
    // use threads to do a bunch of installs in parallel
    do {
      hostsThatDidNotStart.clear();
      //StatusModel.getInstance().queueDeleteAllEntries(); // for gui
      int cnt = STARTINGNODENUMBER;
      for (EC2RegionSpec regionSpec : regionsList) {
        int i;
        for (i = 0; i < regionSpec.getCount(); i++) {
          threads.add(new EC2RunnerThread(runSetName, regionSpec.getRegion(), Integer.toString(cnt), i == 0 ? regionSpec.getIp() : null, timeout));
          cnt = cnt + 1;
        }
      }
      for (Thread thread : threads) {
        thread.start();
      }
      // and wait for all of them to complete
      try {
        for (Thread thread : threads) {
          thread.join();
        }
      } catch (InterruptedException e) {
        System.out.println("Problem joining threads: " + e);
      }

      if (!hostsThatDidNotStart.isEmpty()) {
        System.out.println("Hosts that did not start: " + hostsThatDidNotStart.keySet());
        timeout = (int) ((float) timeout * 1.5);
        System.out.println("Maybe kill them all and try again with timeout " + timeout + "ms?");
        if (showDialog("Hosts that did not start: " + hostsThatDidNotStart.keySet()
                + "\nKill them all and try again with with timeout " + timeout + "ms?"
                + "\nIf you don't respond in 10 seconds this will happen.", 10000)) {
          System.out.println("Yes, kill them all and try again with timeout " + timeout + "ms.");
          terminateRunSet(runSetName);
        } else {
          terminateRunSet(runSetName);
          System.out.println("No, kill them all and quit.");
          return;
        }
      }

      threads.clear();

      // keep repeating until everything starts
    } while (!hostsThatDidNotStart.isEmpty());

    // got a complete set running... now on to step 2
    System.out.println(hostTable.toString());
    // after we know all the hosts are we run the last part

    System.out.println("Hosts that did not start: " + hostsThatDidNotStart.keySet());
    // write out a config file that the GNS installer can use for this set of EC2 hosts
    writeGNSINstallerConf(configName);
    removeShutDownHook();
    System.out.println("Finished creation of Run Set " + runSetName);
  }
  private static final String keyName = "aws";
  // this one installs mondoDB
  private static final String mongoInstallScript = "#!/bin/bash\n"
          + "cd /home/ec2-user\n"
          + "yum --quiet --assumeyes update\n"
          + "yum --quiet --assumeyes install emacs\n" // for debugging
          + "yum --quiet --assumeyes install java-1.7.0-openjdk\n"
          + "echo \\\"[MongoDB]\n" // crazy double escaping for JAVA and BASH going on here!!
          + "name=MongoDB Repository\n"
          + "baseurl=http://downloads-distro.mongodb.org/repo/redhat/os/x86_64\n"
          + "gpgcheck=0\n"
          + "enabled=1\\\" > mongodb.repo\n" // crazy double escaping for JAVA and BASH going on here!!
          + "mv mongodb.repo /etc/yum.repos.d/mongodb.repo\n"
          + "yum --quiet --assumeyes install mongodb-org\n"
          + "service mongod start\n"
          // fix the sudoers so ssh sudo works all the time
          + "chmod ug+rw /etc/sudoers\n"
          + "sed -i 's/requiretty/!requiretty/' /etc/sudoers\n";
//  private static final String mongoInstallScript = "#!/bin/bash\n"
//          + "cd /home/ec2-user\n"
//          + "yum --quiet --assumeyes update\n"
//          + "yum --quiet --assumeyes install emacs\n" // for debugging
//          + "yum --quiet --assumeyes install java-1.7.0-openjdk\n"
//          + "echo \\\"[10gen]\n" // crazy double escaping for JAVA and BASH going on here!!
//          + "name=10gen Repository\n"
//          + "baseurl=http://downloads-distro.mongodb.org/repo/redhat/os/x86_64\n"
//          + "gpgcheck=0\n"
//          + "enabled=1\\\" > 10gen.repo\n" // crazy double escaping for JAVA and BASH going on here!!
//          + "mv 10gen.repo /etc/yum.repos.d/10gen.repo\n"
//          + "yum --quiet --assumeyes install mongo-10gen mongo-10gen-server\n"
//          + "service mongod start";
  private static final String mongoShortInstallScript = "#!/bin/bash\n"
          + "cd /home/ec2-user\n"
          + "yum --quiet --assumeyes update\n"
          //+ "yum --quiet --assumeyes install emacs\n" // for debugging
          + "service mongod start\n";
  private static final String cassandraInstallScript = "#!/bin/bash\n"
          + "cd /home/ubuntu\n" //          + "echo \\\"[datastax]\n"
          //          + "name = DataStax Repo for Apache Cassandra\n"
          //          + "baseurl = http://rpm.datastax.com/community\n"
          //          + "enabled = 1\n"
          //          + "gpgcheck = 0\n\\\" > /etc/yum.repos.d/datastax.repo\n"
          //          + "yum --quiet --assumeyes install dsc12\n"
          //          + "cassandra\n"
          ;
  // older one used to install mySQL
  private static final String mySQLInstallScript
          = "yum --quiet --assumeyes install mysql mysql-server\n"
          + "/etc/init.d/mysqld start\n"
          + "/usr/bin/mysql_install_db \n"
          + "/usr/bin/mysqladmin -u root password 'toorbar'\n"
          + "mysqladmin -u root --password=toorbar -v create gns\n";

  /**
   * This is called to initialize an EC2 host for use as A GNS server in a region. It starts the host, loads all the necessary
   * software and copies the JAR files over. We also collect info about this host, like it's IP address and geographic location.
   * When every host is initialized and we have collected all the IPs, phase two is called.
   *
   * @param region - the EC2 region where we are starting this host
   * @param runSetName - so we can terminate them all together
   * @param id - the GNS ID of this server
   * @param elasticIP
   * @param timeout
   */
  public static void initAndUpdateEC2Host(RegionRecord region, String runSetName, String id, String elasticIP, int timeout) {
    String installScript;
    AMIRecord amiRecord = AMIRecord.getAMI(amiRecordType, region);
    if (amiRecord == null) {
      System.out.println("Invalid combination of " + amiRecordType + " and Region " + region.name());
      return;
    }
    switch (dataStoreType) {
      case CASSANDRA:
        installScript = cassandraInstallScript;
        break;
      default: // MONGO
        if (amiRecordType.toString().contains("Amazon_Linux")) {
          installScript = mongoInstallScript;
        } else {
          switch (amiRecordType) {
            case MongoDB_2_4_8_with_1000_IOPS:
              installScript = mongoShortInstallScript;
              break;
            case Mongo_2014_5_6:
              installScript = null;
              break;
            case Mongo_2014_5_6_micro:
              installScript = null;
              break;
            case Mongo_2015_6_25_vpc:
              installScript = null;
              break;
            default:
              System.out.println("Invalid combination of " + amiRecordType + " and " + dataStoreType);
              return;
          }
        }
    }

    String idString = id.toString();
//    StatusModel.getInstance().queueAddEntry(id); // for the gui
//    StatusModel.getInstance().queueUpdate(id, region.name() + ": [Unknown hostname]", null, null);
    try {
      AWSCredentials credentials = new PropertiesCredentials(new File(CREDENTIALSFILE));
      //Create Amazon Client object
      AmazonEC2 ec2 = new AmazonEC2Client(credentials);
      String nodeName = "GNS Node " + idString;
      System.out.println("Starting install for " + nodeName + " in " + region.name() + " as part of run set " + runSetName);
      HashMap<String, String> tags = new HashMap<>();
      tags.put("runset", runSetName);
      tags.put("id", idString);
//      StatusModel.getInstance().queueUpdate(id, "Creating instance");
      // create an instance
      Instance instance = AWSEC2.createAndInitInstance(ec2, region, amiRecord, nodeName,
              keyName,
              amiRecord.getSecurityGroup(),
              installScript, tags, elasticIP, timeout);
      if (instance != null) {
//        StatusModel.getInstance().queueUpdate(id, "Instance created");
//        StatusModel.getInstance().queueUpdate(id, StatusEntry.State.INITIALIZING);
        // toString our ip
        String hostname = instance.getPublicDnsName();
        InetAddress inetAddress = InetAddress.getByName(hostname);
        String ip = inetAddress.getHostAddress();
        // and take a guess at the location (lat, long) of this host
        Point2D location = GEOLocator.lookupIPLocation(ip);
//        StatusModel.getInstance().queueUpdate(id, hostname, ip, location);
        // update our table of instance information
        hostTable.put(id, new HostInfo(id, hostname, location));

        // and we're done
//        StatusModel.getInstance().queueUpdate(id, "Waiting for other servers");
      } else {
        System.out.println("EC2 Instance " + idString + " in " + region.name() + " did not in start.");
//        StatusModel.getInstance().queueUpdate(id, StatusEntry.State.ERROR, "Did not start");
        hostsThatDidNotStart.put(id, id);
      }
    } catch (IOException e) {
      System.out.println("Problem creating EC2 instance " + idString + " in " + region.name() + ": " + e);
      e.printStackTrace();
    } catch (IllegalArgumentException e) {
      System.out.println("Problem creating EC2 instance " + idString + " in " + region.name() + ": " + e);
      e.printStackTrace();
    }
  }

  private static final String MongoRecordsClass = "edu.umass.cs.gnsserver.database.MongoRecords";
  private static final String CassandraRecordsClass = "edu.umass.cs.gnsserver.database.CassandraRecords";

  /**
   * Terminates all the hosts in the named run set.
   * 
   * @param name
   */
  public static void terminateRunSet(String name) {
    try {
      AWSCredentials credentials = new PropertiesCredentials(new File(CREDENTIALSFILE));
      //Create Amazon Client object
      AmazonEC2 ec2 = new AmazonEC2Client(credentials);
      for (RegionRecord region : RegionRecord.values()) {
        AWSEC2.setRegion(ec2, region);
        for (Instance instance : AWSEC2.getInstances(ec2)) {
          if (!instance.getState().getName().equals(InstanceStateRecord.TERMINATED.getName())) {
            String idString = getTagValue(instance, "id");
            if (name.equals(getTagValue(instance, "runset"))) {
              if (idString != null) {
//                StatusModel.getInstance().queueUpdate(new String(idString), "Terminating");
              }
              AWSEC2.terminateInstance(ec2, instance.getInstanceId());
              if (idString != null) {
//                StatusModel.getInstance().queueUpdate(new String(idString), StatusEntry.State.TERMINATED, "");
              }
            }
          }
        }
      }
    } catch (IOException e) {
      System.out.println("Problem terminating EC2 instances: " + e);
      e.printStackTrace();
    } catch (IllegalArgumentException e) {
      System.out.println("Problem terminating EC2 instances: " + e);
      e.printStackTrace();
    }
  }

  private static void populateIDTableForRunset(String name) {
    AWSCredentials credentials = null;
    try {
      //
      credentials = new PropertiesCredentials(new File(CREDENTIALSFILE));
    } catch (IOException e) {
      System.out.println("Problem contacting EC2 instances: " + e);
    }
    //Create Amazon Client object
    AmazonEC2 ec2 = new AmazonEC2Client(credentials);
    for (RegionRecord region : RegionRecord.values()) {
      AWSEC2.setRegion(ec2, region);
      System.out.println("Retrieving instance information in " + region.name() + "...");
      for (Instance instance : AWSEC2.getInstances(ec2)) {
        if (!instance.getState().getName().equals(InstanceStateRecord.TERMINATED.getName())) {
          String idString = getTagValue(instance, "id");
          if (idString != null && name.equals(getTagValue(instance, "runset"))) {
            String id = new String(idString);
            String hostname = instance.getPublicDnsName();
            String ip = getHostIPSafe(hostname);
            // and take a guess at the location (lat, long) of this host
            Point2D location = GEOLocator.lookupIPLocation(ip);
            hostTable.put(id, new HostInfo(id, hostname, location));
          }
        }
      }
    }
  }

  private static String getHostIPSafe(String hostname) {
    InetAddress inetAddress;
    try {
      inetAddress = InetAddress.getByName(hostname);
      return inetAddress.getHostAddress();
    } catch (UnknownHostException e) {
      return "Unknown";
    }
  }

  private static void describeRunSet(final String name) {
    populateIDTableForRunset(name);
    for (HostInfo info : hostTable.values()) {
      System.out.println(info);
    }
  }

  private static void writeGNSINstallerConfForRunSet(final String name) {
    populateIDTableForRunset(name);
    for (HostInfo info : hostTable.values()) {
      System.out.println(info);
    }
    writeGNSINstallerConf(configName);
  }

  private static String getTagValue(Instance instance, String key) {
    for (Tag tag : instance.getTags()) {
      if (key.equals(tag.getKey())) {
        return tag.getValue();
      }
    }
    return null;
  }
  // COMMAND LINE STUFF
  private static HelpFormatter formatter = new HelpFormatter();
  private static Options commandLineOptions;

  private static CommandLine initializeOptions(String[] args) throws ParseException {
    Option help = new Option("help", "Prints Usage");

    Option terminate = OptionBuilder.withArgName("runSet name").hasArg()
            .withDescription("terminate a runset's instances")
            .create("terminate");
    //Option terminateAll = new Option("terminateAll", "terminate all runsets");
    //Option create = new Option("create", "create a runset");
    Option create = OptionBuilder.withArgName("runSet name").hasArg()
            .withDescription("create a runset")
            .create("create");
    Option describe = OptionBuilder.withArgName("runSet name").hasArg()
            .withDescription("describe a runset")
            .create("describe");
    Option writeConfig = OptionBuilder.withArgName("runSet name").hasArg()
            .withDescription("write a GNS Installer config file for runset")
            .create("writeConfig");
    Option dataStore = OptionBuilder.withArgName("data store type").hasArg()
            .withDescription("data store type")
            .create("datastore");

    commandLineOptions = new Options();
    commandLineOptions.addOption(terminate);
    commandLineOptions.addOption(create);
    commandLineOptions.addOption(describe);
    commandLineOptions.addOption(writeConfig);
    commandLineOptions.addOption(dataStore);
    commandLineOptions.addOption(help);

    CommandLineParser parser = new GnuParser();
    return parser.parse(commandLineOptions, args);
  }

  private static void printUsage() {
    formatter.printHelp("java -cp GNS.jar edu.umass.cs.gnsserver.main.EC2Installer <options>", commandLineOptions);
  }

  private static void startAllMonitoringAndGUIProcesses() {
    java.awt.EventQueue.invokeLater(new Runnable() {
      @Override
      public void run() {
//        StatusFrame.getInstance().setVisible(true);
//        StatusModel.getInstance().addUpdateListener(StatusFrame.getInstance());
//        MapFrame.getInstance().setVisible(true);
//        ScreenUtils.putOnWidestScreen(MapFrame.getInstance());
//        StatusModel.getInstance().addUpdateListener(MapFrame.getInstance());
      }
    });
//    try {
////      new StatusListener().start();
//    } catch (IOException e) {
//      System.out.println("Unable to start Status Listener: " + e.getMessage());
//    }
  }

  private static boolean showDialog(String message, final long timeout) {
    try {
      int dialog = JOptionPane.YES_NO_OPTION;
      JOptionPane optionPane = new JOptionPane(message, JOptionPane.QUESTION_MESSAGE,
              JOptionPane.YES_NO_OPTION);
      final JDialog dlg = optionPane.createDialog("Error");
      new Thread(new Runnable() {
        @Override
        public void run() {
          {
            ThreadUtils.sleep(timeout);
            dlg.dispose();
          }
        }
      }).start();
      dlg.setVisible(true);
      int value = ((Integer) optionPane.getValue()).intValue();
      if (value == JOptionPane.YES_OPTION) {
        return true;
      } else {
        return false;
      }
    } catch (RuntimeException e) {
      return true;
    }
  }

  private static Thread shutdownHook = null;

  private static void attachShutDownHook(final String runSetName) {
    shutdownHook = new Thread() {
      @Override
      public void run() {
        // Just an extra check to make sure we don't kill a perfectly good run set.
        if (!hostsThatDidNotStart.isEmpty()) {
          System.out.println("Killing all instances.");
          terminateRunSet(runSetName);
        }
      }
    };
    Runtime.getRuntime().addShutdownHook(shutdownHook);
    System.out.println("ShutdownHook attached.");
  }

  private static void removeShutDownHook() {
    if (shutdownHook != null) {
      Runtime.getRuntime().removeShutdownHook(shutdownHook);
    }
    shutdownHook = null;
    System.out.println("ShutdownHook removed.");
  }

  /**
   * The main routine.
   * 
   * @param args
   */
  public static void main(String[] args) {
    try {
      CommandLine parser = initializeOptions(args);
      if (parser.hasOption("help")) {
        printUsage();
        System.exit(1);
      }
      String createRunsetName = parser.getOptionValue("create");
      String terminateRunsetName = parser.getOptionValue("terminate");
      String runsetDescribe = parser.getOptionValue("describe");
      String runSetWriteConfig = parser.getOptionValue("writeConfig");
      String dataStoreName = parser.getOptionValue("datastore");

      if (dataStoreName != null) {
        try {
          dataStoreType = DataStoreType.valueOf(dataStoreName);
        } catch (IllegalArgumentException e) {
          System.out.println("Unknown data store type " + dataStoreName + "; exiting.");
          System.exit(1);
        }
      }

      configName = createRunsetName != null ? createRunsetName
              : terminateRunsetName != null ? terminateRunsetName
                      : runsetDescribe != null ? runsetDescribe
                              : runSetWriteConfig != null ? runSetWriteConfig
                                      : null;

      System.out.println("Config name: " + configName);
      if (configName != null) {
        loadConfig(configName);
      }

      if (createRunsetName != null) {
        createRunSetMulti(createRunsetName);
      } else if (terminateRunsetName != null) {
        terminateRunSet(terminateRunsetName);
      } else if (runsetDescribe != null) {
        describeRunSet(runsetDescribe);
      } else if (runSetWriteConfig != null) {
        writeGNSINstallerConfForRunSet(runSetWriteConfig);
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

  private static class EC2RunnerThread extends Thread {

    String runSetName;
    RegionRecord region;
    String id;
    String ip;
    int timeout;

    public EC2RunnerThread(String runSetName, RegionRecord region, String id, String ip, int timeout) {
      super("Install Start " + id);
      this.runSetName = runSetName;
      this.region = region;
      this.id = id;
      this.ip = ip;
      this.timeout = timeout;
    }

    @Override
    public void run() {
      EC2Runner.initAndUpdateEC2Host(region, runSetName, id, ip, timeout);
    }
  }

  private static void writeGNSINstallerConf(String configName) {
    File jarPath = getJarPath();
    System.out.println("Jar path: " + jarPath);
    String confFileDirectory = jarPath.getParent() + FILESEPARATOR + "conf" + FILESEPARATOR + configName + "-init";
    //String confFileLocation = jarPath.getParent() + FILESEPARATOR + "conf" + FILESEPARATOR + "gnsInstaller" + FILESEPARATOR + configName + ".xml";
    WriteConfFile.writeConfFiles(configName, confFileDirectory, keyName, ec2UserName, "linux", "MONGO", hostTable);
    //WriteConfFile.writeXMLFile(confFileLocation, keyName, ec2UserName, "linux", dataStoreType.toString(), hostTable);
  }

  private static File getJarPath() {
    try {
      return new File(GNS.class.getProtectionDomain().getCodeSource().getLocation().toURI());
    } catch (URISyntaxException e) {
      GNS.getLogger().info("Unable to get jar location: " + e);
      return null;
    }
  }

}
