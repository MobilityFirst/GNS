package edu.umass.cs.gns.main;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Tag;
import edu.umass.cs.amazontools.AMIRecord;
import edu.umass.cs.amazontools.AWSEC2;
import edu.umass.cs.amazontools.InstanceStateRecord;
import edu.umass.cs.amazontools.RegionRecord;
import edu.umass.cs.amazontools.SSHClient;
import edu.umass.cs.gns.statusdisplay.MapFrame;
import edu.umass.cs.gns.statusdisplay.StatusEntry;
import edu.umass.cs.gns.statusdisplay.StatusFrame;
import edu.umass.cs.gns.statusdisplay.StatusListener;
import edu.umass.cs.gns.statusdisplay.StatusModel;
import edu.umass.cs.gns.util.ConfigFileInfo;
import edu.umass.cs.gns.util.Format;
import edu.umass.cs.gns.util.GEOLocator;
import edu.umass.cs.gns.util.ScreenUtils;
import edu.umass.cs.gns.util.XMLParser;
import java.awt.geom.Point2D;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.prefs.Preferences;
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
 * java -cp GNS.jar edu.umass.cs.gns.main.EC2Installer -config "release-config" -update "release"
 *
 * @author westy
 */
public class EC2Installer {

  private static final String NEWLINE = System.getProperty("line.separator");
  private static String EC2USERNAME = "ec2-user";
  private static final String FILESEPARATOR = System.getProperty("file.separator");
  private static final String PRIVATEKEYFILEEXTENSION = ".pem";
  //private static final String PUBLICKEYFILEEXTENSION = ".pub";
  private static final String KEYHOME = System.getProperty("user.home") + FILESEPARATOR + ".ssh";
  private static final String CREDENTIALSFILE = System.getProperty("user.home") + FILESEPARATOR + "AwsCredentials.properties";
  //private static AmazonEC2 ec2;
  private static Preferences preferences = Preferences.userRoot().node(EC2Installer.class.getName());
  /**
   * Contains information read from config file on what hosts we are trying to start.
   */
  private static List<RegionSpec> regionsList = new ArrayList<RegionSpec>();
  /**
   * Stores information about instances that have started.
   */
  private static ConcurrentHashMap<Integer, InstanceInfo> idTable = new ConcurrentHashMap<Integer, InstanceInfo>();
  //
  private static final int STARTINGNODENUMBER = 0;
  private static ConcurrentHashMap<Integer, Integer> hostsThatDidNotStart = new ConcurrentHashMap<Integer, Integer>();
  private static DataStoreType dataStoreType = DataStoreType.MONGO;

  /**
   * Information read from config file on what hosts we are trying to start.
   */
  private static class RegionSpec {

    private RegionRecord region;
    private int cnt;
    private String elasticIP;

    public RegionSpec(RegionRecord region, int cnt, String elasticIP) {
      this.region = region;
      this.cnt = cnt;
      this.elasticIP = elasticIP;
    }

    public RegionRecord getRegion() {
      return region;
    }

    public int getCnt() {
      return cnt;
    }

    public String getElasticIP() {
      return elasticIP;
    }
  }

  /**
   * Information about instances that have started
   */
  private static class InstanceInfo {

    private int id;
    private String hostname;
    private String ip;
    private Point2D location;

    public InstanceInfo(int id, String hostname, String ip, Point2D location) {
      this.id = id;
      this.hostname = hostname;
      this.ip = ip;
      this.location = location;
    }

    public int getId() {
      return id;
    }

    public String getHostname() {
      return hostname;
    }

    public String getIp() {
      return ip;
    }

    public Point2D getLocation() {
      return location;
    }

    @Override
    public String toString() {
      return "InstanceInfo{" + "id=" + id + ", hostname=" + hostname + ", ip=" + ip + ", location=" + location + '}';
    }
  }

  private static void loadConfig(String configName) {
    XMLParser ec2parse = new XMLParser(configName);
    for (int i = 0; i < ec2parse.size(); i++) {
      String region = ec2parse.getAttribute(i, "name", true);
      int cnt = Integer.parseInt(ec2parse.getAttribute(i, "cnt", true));
      String elasticIP = ec2parse.getAttribute(i, "ip");
      System.out.println(region + " " + cnt + " " + elasticIP);
      regionsList.add(new RegionSpec(RegionRecord.valueOf(region), cnt, elasticIP));
    }
  }

  /**
   * Starts a set of EC2 hosts running GNS that we call a runset.
   */
  public static void createRunSetMulti(String runSetName) {
    preferences.put(RUNSETNAME, runSetName); // store the last one
    startAllMonitoringAndGUIProcesses();
    StatusModel.getInstance().queueDeleteAllEntries(); // for gui
    ArrayList<Thread> threads = new ArrayList<Thread>();
    //String runSetName = nextRunSetName();
    // use threads to do a bunch of installs in parallel
    int cnt = STARTINGNODENUMBER;
    for (RegionSpec regionSpec : regionsList) {
      int i;
      for (i = 0; i < regionSpec.getCnt(); i++) {
        threads.add(new InstallStartThread(runSetName, regionSpec.getRegion(), cnt, i == 0 ? regionSpec.elasticIP : null));
        cnt = cnt + 1;
      }
    }
    // start em all
    for (int i = 0; i < threads.size(); i++) {
      threads.get(i).start();
    }
    // and wait for all of them to complete
    try {
      for (int i = 0; i < threads.size(); i++) {
        threads.get(i).join();
      }
    } catch (Exception e) {
      System.out.println("Problem joining threads: " + e);
    }

    System.out.println("Hosts that did not start: " + hostsThatDidNotStart.keySet());
    System.out.println(idTable.toString());
    // after we know all the hosts are we run the last part
    threads.clear();

    // now start all the finishing threads
    for (InstanceInfo info : idTable.values()) {
      System.out.println("Finishing install for " + info.getHostname());
      //GNS.getLogger().info("Finishing install for " + entry.getKey());
      threads.add(new InstallFinishThread(info.getId(), info.getHostname()));
    }
    for (int i = 0; i < threads.size(); i++) {
      threads.get(i).start();
    }
    // and wait form the to complete
    try {
      for (int i = 0; i < threads.size(); i++) {
        threads.get(i).join();
      }
    } catch (Exception e) {
      System.out.println("Problem joining threads: " + e);
    }

    System.out.println("Hosts that did not start: " + hostsThatDidNotStart.keySet());
    System.out.println("Finished creation of Run Set " + runSetName);
    // do some bookeeping
//    HashSet<String> hosts = new HashSet<String>();
//    for (InstanceInfo info : idTable.values()) {
//      hosts.add(info.getHostname());
//    }
//    runSetHosts.put(runSetName, hosts);
    // now we send out packets telling all the hosts where to send
    // there status updates
    // update the config info so no where to send stuff
    try {
      for (InstanceInfo info : idTable.values()) {
        InetAddress ipAddress = InetAddress.getByName(info.getHostname());
        ConfigFileInfo.addHostInfo(info.getId(), ipAddress, GNS.startingPort, 0, info.getLocation().getY(), info.getLocation().getX());
      }
    } catch (UnknownHostException e) {
      System.err.println("Problem parsing IP address " + e);
    }
    StatusListener.sendOutServerInitPackets(idTable.keySet());
  }
  private static final String keyName = "aws";
  private static final String GNSJar = "/Users/westy/Documents/Code/GNS/build/jars/GNS.jar";
  private static String GNSFile = new File(GNSJar).getName();
  // this one installs mondoDB
  private static final String mongoInstallScript = "#!/bin/bash\n"
          + "cd /home/ec2-user\n"
          + "yum --quiet --assumeyes update\n"
          + "yum --quiet --assumeyes install emacs\n" // for debugging
          + "echo \\\"[10gen]\n" // crazy double escaping for JAVA and BASH going on here!!
          + "name=10gen Repository\n"
          + "baseurl=http://downloads-distro.mongodb.org/repo/redhat/os/x86_64\n"
          + "gpgcheck=0\n"
          + "enabled=1\\\" > 10gen.repo\n" // crazy double escaping for JAVA and BASH going on here!!
          + "mv 10gen.repo /etc/yum.repos.d/10gen.repo\n"
          + "yum --quiet --assumeyes install mongo-10gen mongo-10gen-server\n"
          + "service mongod start";
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
  private static final String mySQLInstallScript =
          "yum --quiet --assumeyes install mysql mysql-server\n"
          + "/etc/init.d/mysqld start\n"
          + "/usr/bin/mysql_install_db \n"
          + "/usr/bin/mysqladmin -u root password 'toorbar'\n"
          + "mysqladmin -u root --password=toorbar -v create gns";

  //private static final String installScript = baseInstallScript + mongoInstallScript + cassandraInstallScript;
  /**
   * This is called to initialize an EC2 host for use as A GNS server in a region. It starts the host, loads all the necessary
   * software and copies the JAR files over. We also collect info about this host, like it's IP address and geographic location.
   * When every host is initialized and we have collected all the IPs, phase two is called.
   *
   * @param region - the EC2 region where we are starting this host
   * @param runSetName - so we can terminate them all together
   * @param id - the GNS ID of this server
   */
  public static void installPhaseOne(RegionRecord region, String runSetName, int id, String elasticIP) {
    String installScript;
    AMIRecord ami;
    switch (dataStoreType) {
      case MONGO:
        installScript = mongoInstallScript;
        EC2USERNAME = "ec2-user";
        ami = region.getDefaultAMI();
        break;
      case CASSANDRA:
        installScript = cassandraInstallScript;
        EC2USERNAME = "ubuntu";
        ami = region.getCassandraAMI();
        break;
      default:
        installScript = mongoInstallScript;
        EC2USERNAME = "ec2-user";
        ami = region.getDefaultAMI();
    }

    String idString = Integer.toString(id);
    StatusModel.getInstance().queueAddEntry(id); // for the gui
    StatusModel.getInstance().queueUpdate(id, region.name() + ": [Unknown hostname]", null, null);
    try {
      AWSCredentials credentials = new PropertiesCredentials(new File(CREDENTIALSFILE));
      //Create Amazon Client object
      AmazonEC2 ec2 = new AmazonEC2Client(credentials);
      String nodeName = "GNS Node " + idString;
      System.out.println("Starting install for " + nodeName + " in " + region.name() + " as part of run set " + runSetName);
      HashMap tags = new HashMap();
      tags.put("runset", runSetName);
      tags.put("id", idString);
      StatusModel.getInstance().queueUpdate(id, "Creating instance");
      // create an instance
      Instance instance = AWSEC2.createAndInitInstance(ec2, region, ami, nodeName, keyName, installScript, tags, elasticIP);
      if (instance != null) {
        StatusModel.getInstance().queueUpdate(id, "Instance created");
        StatusModel.getInstance().queueUpdate(id, StatusEntry.State.INITIALIZING);
        // get our ip
        String hostname = instance.getPublicDnsName();
        InetAddress inetAddress = InetAddress.getByName(hostname);
        String ip = inetAddress.getHostAddress();
        // and take a guess at the location (lat, long) of this host
        Point2D location = GEOLocator.lookupIPLocation(ip);
        StatusModel.getInstance().queueUpdate(id, hostname, ip, location);
        // move the JAR files over
        copyJARFiles(id, hostname);
        // update our table of instance information
        idTable.put(id, new InstanceInfo(id, hostname, ip, location));
        // store the hostname on preferences so we can access it later
        storeHostname(runSetName, id, hostname);

        // and we're done
        StatusModel.getInstance().queueUpdate(id, "Waiting for other servers");
      } else {
        System.out.println("EC2 Instance " + idString + " in " + region.name() + " did not in start.");
        StatusModel.getInstance().queueUpdate(id, StatusEntry.State.ERROR, "Did not start");
        hostsThatDidNotStart.put(id, id);
      }
    } catch (Exception e) {
      System.out.println("Problem creating EC2 instance " + idString + " in " + region.name() + ": " + e);
      e.printStackTrace();
    }
  }

  /**
   * This is called to finish the host setup after all the hosts are up and running. The name-server-info file is created using all
   * the IP address of all the hosts. Then the various servers are started on the host.
   *
   * @param id
   * @param hostname
   */
  public static void installPhaseTwo(int id, String hostname) {
    File keyFile = new File(KEYHOME + FILESEPARATOR + keyName + PRIVATEKEYFILEEXTENSION);
    // write the name-server-info
    StatusModel.getInstance().queueUpdate(id, "Creating name-server-info");
    writeNSFile(hostname, keyFile);
    startServers(id, hostname);
  }

  private static void copyJARFiles(int id, String hostname) {
    File keyFile = new File(KEYHOME + FILESEPARATOR + keyName + PRIVATEKEYFILEEXTENSION);
    StatusModel.getInstance().queueUpdate(id, "Copying jar files");
    SSHClient.scpTo(EC2USERNAME, hostname, keyFile, GNSJar, GNSFile);
  }
  private static final String MongoRecordsClass = "edu.umass.cs.gns.database.MongoRecords";
  private static final String CassandraRecordsClass = "edu.umass.cs.gns.database.CassandraRecords";

  private static void deleteDatabase(int id, String hostname) {
    File keyFile = new File(KEYHOME + FILESEPARATOR + keyName + PRIVATEKEYFILEEXTENSION);
    AWSEC2.executeBashScript(hostname, keyFile, "deleteDatabase.sh",
            "#!/bin/bash\n"
            + "java -cp " + GNSFile + " " + MongoRecordsClass + " -clear");
  }
  private static final String StartLNSClass = "edu.umass.cs.gns.main.StartLocalNameServer";
  private static final String StartNSClass = "edu.umass.cs.gns.main.StartNameServer";
  private static final String StartHTTPServerClass = "edu.umass.cs.gns.httpserver.GnsHttpServer";

  /**
   * Starts an LNS, NS and HTTP server on the host.
   *
   * @param id
   * @param hostname
   */
  private static void startServers(int id, String hostname) {
    StatusModel.getInstance().queueUpdate(id, "Starting local name servers");
    File keyFile = new File(KEYHOME + FILESEPARATOR + keyName + PRIVATEKEYFILEEXTENSION);
    AWSEC2.executeBashScript(hostname, keyFile, "runLNS.sh",
            "#!/bin/bash\n"
            + "mv --backup=numbered LNSlogfile LNSlogfile.save\n"
            + "nohup java -cp " + GNSFile + " " + StartLNSClass + " "
            + "-id " + id
            // at some point a bunch of these should become defaults
            + " -cacheSize 10000 "
            + " -primary 3 -location -vInterval 1000"
            + " -chooseFromClosestK 1 -lookupRate 10000 -updateRateMobile 0 -updateRateRegular 10000 "
            + " -numberOfTransmissions 3 -maxQueryWaitTime 100000 -queryTimeout 100 "
            //+ " -adaptiveTimeout -delta 0.05 -mu 1.0 -phi 6.0 "
            + " -fileLoggingLevel FINE -consoleOutputLevel FINE -statFileLoggingLevel INFO -statConsoleOutputLevel INFO "
            + " -debugMode "
            + " -nsfile name-server-info  > LNSlogfile 2>&1 &");

    StatusModel.getInstance().queueUpdate(id, "Starting name servers");
    AWSEC2.executeBashScript(hostname, keyFile, "runNS.sh",
            "#!/bin/bash\n"
            + "mv --backup=numbered NSlogfile NSlogfile.save\n"
            + "nohup java -cp " + GNSFile + " " + StartNSClass + " "
            + " -id " + id
            // at some point a bunch of these should become defaults
            + " -primary 3 -aInterval 1000 -rInterval 1000 -nconstant 0.1 -mavg 20 -ttlconstant 0.0 -rttl 0 -mttl 0"
            + " -rworkload 0 -mworkload 0"
            + " -location -nsVoteSize 5 "
            + " -fileLoggingLevel FINE -consoleOutputLevel FINE -statFileLoggingLevel INFO -statConsoleOutputLevel INFO"
            + " -dataStore " + dataStoreType.name()
            + " -debugMode "
            + " -nsfile name-server-info "
            + "> NSlogfile 2>&1 &");
    StatusModel.getInstance().queueUpdate(id, "Starting HTTP servers");
    AWSEC2.executeBashScript(hostname, keyFile, "runHTTP.sh",
            "#!/bin/bash\n"
            + "mv --backup=numbered HTTPlogfile HTTPlogfile.save\n"
            + "nohup java -cp " + GNSFile + " " + StartHTTPServerClass + " "
            + "-lnsid " + id + " -nsfile name-server-info  > HTTPlogfile 2>&1 &",
            id);
    StatusModel.getInstance().queueUpdate(id, StatusEntry.State.RUNNING, "All servers started");
  }

  private static void killAllServers(int id, String hostname) {
    File keyFile = new File(KEYHOME + FILESEPARATOR + keyName + PRIVATEKEYFILEEXTENSION);
    StatusModel.getInstance().queueUpdate(id, "Killing servers");
    AWSEC2.executeBashScript(hostname, keyFile, "killAllServers.sh", "#!/bin/bash\nkillall java");
  }

  /**
   * Write the name-server-info file on the remote host.
   *
   * @param hostname
   * @param keyFile
   */
  private static void writeNSFile(String hostname, File keyFile) {
    StringBuilder result = new StringBuilder();
    //HostID IsNS? IPAddress [StartingPort | - ] Ping-Latency Latitude Longitude
    for (InstanceInfo info : idTable.values()) {
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
    SSHClient.execWithSudoNoPass(EC2USERNAME, hostname, keyFile, "echo \"" + result.toString() + "\" > name-server-info");
  }

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
                StatusModel.getInstance().queueUpdate(Integer.parseInt(idString), "Terminating");
              }
              AWSEC2.terminateInstance(ec2, instance.getInstanceId());
              if (idString != null) {
                StatusModel.getInstance().queueUpdate(Integer.parseInt(idString), StatusEntry.State.TERMINATED, "");
              }
            }
          }
        }
      }
    } catch (Exception e) {
      System.out.println("Problem terminating EC2 instances: " + e);
      e.printStackTrace();
    }
  }

  private static void terminateAllRunSets() {
    try {
      AWSCredentials credentials = new PropertiesCredentials(new File(CREDENTIALSFILE));
      //Create Amazon Client object
      AmazonEC2 ec2 = new AmazonEC2Client(credentials);
      for (RegionRecord region : RegionRecord.values()) {
        AWSEC2.setRegion(ec2, region);
        for (Instance instance : AWSEC2.getInstances(ec2)) {
          if (!instance.getState().getName().equals(InstanceStateRecord.TERMINATED.getName())) {
            if (getTagValue(instance, "runset") != null) {
              AWSEC2.terminateInstance(ec2, instance.getInstanceId());
            }
          }
        }
      }
    } catch (Exception e) {
      System.out.println("Problem terminating EC2 instances: " + e);
      e.printStackTrace();
    }
  }

  public enum UpdateAction {

    UPDATE,
    RESTART,
    DELETE_DATABASE
  };

  /**
   * Copies the latest version of the JAR files to the all the hosts in the runset given by name and restarts all the servers.
   *
   * @param name
   */
  public static void updateRunSet(String name, UpdateAction action) {
    ArrayList<Thread> threads = new ArrayList<Thread>();
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
            int id = Integer.parseInt(idString);
            String hostname = instance.getPublicDnsName();
            //String hostname = retrieveHostname(name, id);
            threads.add(new UpdateThread(id, hostname, action));
          }
        }
      }
    }
    for (int i = 0; i < threads.size(); i++) {
      threads.get(i).start();
    }
    // and wait for them to complete
    try {
      for (int i = 0; i < threads.size(); i++) {
        threads.get(i).join();
      }
    } catch (Exception e) {
      System.out.println("Problem joining threads: " + e);
    }
  }

  public static void describeRunSet(final String name) {
    try {
      AWSCredentials credentials = new PropertiesCredentials(new File(CREDENTIALSFILE));
      //Create Amazon Client object
      AmazonEC2 ec2 = new AmazonEC2Client(credentials);
      for (RegionRecord region : RegionRecord.values()) {
        AWSEC2.setRegion(ec2, region);
        for (Instance instance : AWSEC2.getInstances(ec2)) {
          if (!instance.getState().getName().equals(InstanceStateRecord.TERMINATED.getName())) {
            final String idString = getTagValue(instance, "id");
            if (idString != null && name.equals(getTagValue(instance, "runset"))) {
              int id = Integer.parseInt(idString);
              String hostname = instance.getPublicDnsName();
              //String hostname = retrieveHostname(name, id);
              System.out.println("Node " + id + " running on " + hostname);
            }
          }
        }
      }
    } catch (Exception e) {
      System.out.println("Problem contacting EC2 instances: " + e);
      e.printStackTrace();
    }
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
    //Option createOne = new Option("createOne", "create one runset test");
    //Option updateCurrent = new Option("updateCurrent", "update current runset");
    Option update = OptionBuilder.withArgName("runSet name").hasArg()
            .withDescription("update a runset")
            .create("update");
    Option restart = OptionBuilder.withArgName("runSet name").hasArg()
            .withDescription("restart a runset")
            .create("restart");
    Option deleteDatabase = OptionBuilder.withArgName("runSet name").hasArg()
            .withDescription("delete the databases in a runset")
            .create("deleteDatabase");
    Option describe = OptionBuilder.withArgName("runSet name").hasArg()
            .withDescription("describe a runset")
            .create("describe");
    Option configName = OptionBuilder.withArgName("config name").hasArg()
            .withDescription("configuration file name")
            .create("config");
    Option dataStore = OptionBuilder.withArgName("data store type").hasArg()
            .withDescription("data store type")
            .create("datastore");

    commandLineOptions = new Options();
    commandLineOptions.addOption(terminate);
    //commandLineOptions.addOption(terminateAll);
    commandLineOptions.addOption(create);
    //commandLineOptions.addOption(createOne);
    commandLineOptions.addOption(update);
    commandLineOptions.addOption(restart);
    commandLineOptions.addOption(deleteDatabase);
    //commandLineOptions.addOption(updateCurrent);
    commandLineOptions.addOption(describe);
    commandLineOptions.addOption(configName);
    commandLineOptions.addOption(dataStore);
    commandLineOptions.addOption(help);

    CommandLineParser parser = new GnuParser();
    return parser.parse(commandLineOptions, args);
  }

  private static void printUsage() {
    formatter.printHelp("Installer", commandLineOptions);
  }

  private static void printUsage(String header) {
    formatter.printHelp("java -cp GNS.jar edu.umass.cs.gns.main.EC2Installer <options>", header, commandLineOptions, "");
  }

  private static void startAllMonitoringAndGUIProcesses() {
    java.awt.EventQueue.invokeLater(new Runnable() {
      @Override
      public void run() {
        StatusFrame.getInstance().setVisible(true);
        StatusModel.getInstance().addUpdateListener(StatusFrame.getInstance());
        MapFrame.getInstance().setVisible(true);
        ScreenUtils.putOnWidestScreen(MapFrame.getInstance());
        StatusModel.getInstance().addUpdateListener(MapFrame.getInstance());
      }
    });
    try {
      new StatusListener().start();
    } catch (Exception e) {
      System.out.println("Unable to start Status Listener: " + e.getMessage());
    }
  }

  public static void main(String[] args) {
    //loadConfig();
    //System.out.println("Current Run Set is " + currentRunSetName());
    try {
      CommandLine parser = initializeOptions(args);
      if (parser.hasOption("help")) {
        printUsage();
        System.exit(1);
      }
      //Boolean create = parser.hasOption("create");
      String createRunsetName = parser.getOptionValue("create");
      //Boolean createOne = parser.hasOption("createOne");
      //Boolean terminateAll = parser.hasOption("terminateAll");
      String terminateRunsetName = parser.getOptionValue("terminate");
      //Boolean updateCurrent = parser.hasOption("updateCurrent");
      String runsetUpdate = parser.getOptionValue("update");
      String runsetRestart = parser.getOptionValue("restart");
      String runsetDeleteDatabase = parser.getOptionValue("deleteDatabase");
      String runsetDescribe = parser.getOptionValue("describe");
      String configName = parser.getOptionValue("config");
      String dataStoreName = parser.getOptionValue("datastore");

      if (dataStoreName != null) {
        try {
          dataStoreType = DataStoreType.valueOf(dataStoreName);
        } catch (IllegalArgumentException e) {
          System.out.println("Unknown data store type " + dataStoreName + "; exiting.");
          System.exit(1);
        }
      }


      if (configName != null) {
        loadConfig(configName);
      } else if (createRunsetName != null) {
        printUsage("-config must be specified with create");
        System.exit(1);
      }

      if (createRunsetName != null) {
        createRunSetMulti(createRunsetName);
//      } else if (createOne) {
//        createRunSet();
      } else if (terminateRunsetName != null) {
        terminateRunSet(terminateRunsetName);
//      } else if (terminateAll) {
//        terminateAllRunSets();
      } else if (runsetUpdate != null) {
        updateRunSet(runsetUpdate, UpdateAction.UPDATE);
      } else if (runsetRestart != null) {
        updateRunSet(runsetRestart, UpdateAction.RESTART);
      } else if (runsetDeleteDatabase != null) {
        updateRunSet(runsetDeleteDatabase, UpdateAction.DELETE_DATABASE);
      } else if (runsetDescribe != null) {
        describeRunSet(runsetDescribe);
      } else {
        printUsage();
        System.exit(1);
      }

    } catch (Exception e1) {
      e1.printStackTrace();
      printUsage();
      System.exit(1);
    }
    System.exit(0);
  }

  static class InstallStartThread extends Thread {

    String runSetName;
    RegionRecord region;
    int id;
    String ip;

    public InstallStartThread(String runSetName, RegionRecord region, int id, String ip) {
      super("Install Start " + id);
      this.runSetName = runSetName;
      this.region = region;
      this.id = id;
      this.ip = ip;
    }

    @Override
    public void run() {
      EC2Installer.installPhaseOne(region, runSetName, id, ip);
    }
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
      EC2Installer.installPhaseTwo(id, hostname);
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
          copyJARFiles(id, hostname);
          break;
        case RESTART:
          break;
        case DELETE_DATABASE:
          deleteDatabase(id, hostname);
          break;
      }
      startServers(id, hostname);
      System.out.println("#### Node " + id + " running on " + hostname + " finished update ####");
    }
  }
  //
  private static final String RUNSETNAME = "RUNSETNAME";

//  private static String nextRunSetName() {
//    String runSetNumber = preferences.get(RUNSETNAME, "1");
//    Integer next = Integer.parseInt(runSetNumber);
//    next++;
//    String nextString = next.toString();
//    preferences.put(RUNSETNAME, nextString);
//    return nextString;
//  }
  public static String currentRunSetName() {
    return preferences.get(RUNSETNAME, "1");
  }

  private static void storeHostname(String runSetName, int id, String hostname) {
    preferences.put(runSetName + "-" + id, hostname);
  }

  public static String retrieveHostname(String runSetName, int id) {
    return preferences.get(runSetName + "-" + id, null);
  }
}
