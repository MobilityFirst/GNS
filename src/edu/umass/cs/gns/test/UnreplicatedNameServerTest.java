package edu.umass.cs.gns.test;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartLocalNameServer;
import edu.umass.cs.gns.nsdesign.GNSNodeConfig;
import edu.umass.cs.gns.nsdesign.NameServer;
import edu.umass.cs.gns.util.ConsistentHashing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

// fixme delete this class

/**
 * Runs tests for a single name server without coordination among multiple replicas.
 * It creates one local name server to send requests.
 *
 * Created by abhigyan on 2/28/14.
 */
public class UnreplicatedNameServerTest {

  public static void main(String[] args) {
    GNS.statConsoleOutputLevel = GNS.consoleOutputLevel = GNS.fileLoggingLevel = GNS.statFileLoggingLevel = "FINE";
    try {
      String nodeConfigFile = "conf/singleNStest/node_config_1ns_1lns";
      String nsConfigFile = "conf/singleNStest/gns-ns.conf";
      String lnsConfigFile = "conf/singleNStest/gns-lns.conf";
      int nameserverID = 0;
      int lnsID = 1;
      HashSet<Integer> nameServerIDs = new HashSet<Integer>();
      nameServerIDs.add(nameserverID);
      ConsistentHashing.initialize(1, nameServerIDs);

      GNSNodeConfig nodeConfig = new GNSNodeConfig(nodeConfigFile, nameserverID);
      NameServer nameServer = new NameServer(0, nsConfigFile, nodeConfig);
//      nameServer.reset();
      GNS.getLogger().info("Name server created ..");

      StartLocalNameServer.startLNSConfigFile(lnsID, nodeConfigFile, lnsConfigFile, null);

      String name = "abhigyan";
      List<TestRequest> testRequest = new ArrayList<TestRequest>();
      testRequest.add(new TestRequest(name, TestRequest.ADD));
      testRequest.add(new TestRequest(name, TestRequest.REMOVE));
//      testRequest.add(new TestRequest(name, TestRequest.ADD)); // this should fail
//      // todo is there a way to test responses automatically? e.g., do a lookup after update to test if it works.
//      testRequest.add(new TestRequest(name, TestRequest.LOOKUP));
//      testRequest.add(new TestRequest("abcddfad", TestRequest.LOOKUP));
//
//      testRequest.add(new TestRequest(name, TestRequest.UPDATE));
//
//      testRequest.add(new TestRequest("qwserty", TestRequest.UPDATE));
//
//      testRequest.add(new TestRequest(name, TestRequest.REMOVE));
//
//      testRequest.add(new TestRequest(name, TestRequest.REMOVE));
//
//      testRequest.add(new TestRequest(name, TestRequest.REMOVE));

//      new RequestGenerator().generateRequests(testRequest, 5000.0, LocalNameServer.getExecutorService());

      GNS.getLogger().info("Local name server started ...");
    } catch (IOException e) {
      GNS.getLogger().info("ERROR: Test unsuccessful due to exception.");

      e.printStackTrace();
      System.exit(2);
    }

  }
}
