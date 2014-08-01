package edu.umass.cs.gns.paxos;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.Config;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by abhigyan on 2/13/14.
 */
public class TestConfig {

  int numPaxosReplicas;

  int startingPort = 23434;
//  boolean debuggingEnabled;
  int maxThreads;

  int MEMORY_GARBAGE_COLLECTION_INTERVAL = 100; // milliseconds

  String testPaxosLogFolder = "paxosLog";

  String testPaxosID = "testPaxosID";

  int numberRequests = 100;

  int testDurationSeconds = 10;

  String outputFolder = "paxosTestOutput";

  public TestConfig(String testConfigFile) {
    readTestConfigFile(testConfigFile);
  }

  /**
   * read config file during testing/debugging
   * @param testConfig configuration file used during testing
   */
  private  void readTestConfigFile(String testConfig) {
    File f = new File(testConfig);
    if (!f.exists()) {
      if (Config.debuggingEnabled) GNS.getLogger().fine(" testConfig file does not exist. Quit. " +
              "Filename =  " + testConfig);
      System.exit(2);
    }

    try {
    Properties prop = new Properties();

      InputStream input = new FileInputStream(testConfig);
      // load a properties file
      prop.load(input);

      if (prop.containsKey("NumberOfReplicas"))  numPaxosReplicas = Integer.parseInt(prop.getProperty("NumberOfReplicas"));

      if (prop.containsKey("EnableLogging")) Config.debuggingEnabled = Boolean.parseBoolean(prop.getProperty("EnableLogging"));

      if (prop.containsKey("MaxThreads")) maxThreads = Integer.parseInt(prop.getProperty("MaxThreads"));

      if (prop.containsKey("GarbageCollectionInterval"))
        MEMORY_GARBAGE_COLLECTION_INTERVAL = Integer.parseInt(prop.getProperty("GarbageCollectionInterval"));

      if (prop.containsKey("PaxosLogFolder")) testPaxosLogFolder = prop.getProperty("PaxosLogFolder");

      if (prop.containsKey("NumberOfRequests")) numberRequests = Integer.parseInt(prop.getProperty("NumberOfRequests"));

      if (prop.containsKey("TestDurationSeconds")) testDurationSeconds = Integer.parseInt(prop.getProperty("TestDurationSeconds"));

      if (prop.containsKey("OutputFolder")) outputFolder = prop.getProperty("OutputFolder");
    } catch (IOException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
  }

  public static void main(String[] args) {
    TestConfig config = new TestConfig("resources/testCodeResources/testConfig");

  }

}
