package edu.umass.cs.gns.test;

import edu.umass.cs.gns.localnameserver.LocalNameServer;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartLocalNameServer;
import edu.umass.cs.gns.nsdesign.GNSNodeConfig;
import edu.umass.cs.gns.nsdesign.NameServer;
import edu.umass.cs.gns.workloads.Constant;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Test for a GNS consisting of three name servers and a local name server.
 *
 * Created by abhigyan on 3/30/14.
 */
public class MultiNameServerTest {

  public static void main(String[] args) {
    GNS.statConsoleOutputLevel = GNS.consoleOutputLevel = GNS.fileLoggingLevel = GNS.statFileLoggingLevel = "FINE";
    try {
      String nodeConfigFile = "conf/multiNStest/node_config_3ns_1lns";
      String nsConfigFile = "conf/multiNStest/gns-ns.conf";
      String lnsConfigFile = "conf/multiNStest/gns-lns.conf";

      String gnsLog = "gnsLog";
      String rcLogFolder = "rcPaxosLog";
      String gnsLogFolder = "gnsPaxosLog";
      File f = new File(gnsLog);
      if (f.exists()){
        deleteDir(f);
      }
      f = new File(rcLogFolder);
      if (f.exists()){
        deleteDir(f);
      }
      f = new File(gnsLogFolder);
      if (f.exists()) {
        deleteDir(f);
      }
      GNSNodeConfig nodeConfig = new GNSNodeConfig(nodeConfigFile, 0);

      int count = 0;
      for (int nsID: nodeConfig.getNameServerIDs()) {
        GNSNodeConfig myNodeConfig = new GNSNodeConfig(nodeConfigFile, nsID);
        NameServer ns = new NameServer(nsID, nsConfigFile, myNodeConfig);
//        ns.reset();

        GNS.getLogger().info("Running name server " + nsID);
        count += 1;
      }
      assert  count == 3;
//      nameServer.reset();
      GNS.getLogger().info("Name server created ..");

      int lnsID = nodeConfig.getNameServerIDs().size();
      StartLocalNameServer.startLNSConfigFile(lnsID, nodeConfigFile, lnsConfigFile, null);

      GNS.getLogger().info("Local name server started ...");

//      String name = "abhigyan";
      ArrayList<TestRequest> testRequest = new ArrayList<TestRequest>();
//      testRequest.add(new TestRequest(name, TestRequest.ADD));

//      testRequest.add(new TestRequest(name, TestRequest.LOOKUP));
//      testRequest.add(new TestRequest(name, TestRequest.UPDATE));
//      testRequest.add(new TestRequest(name, TestRequest.LOOKUP));
//      testRequest.add(new TestRequest(name, TestRequest.REMOVE));

//      testRequest.add(new TestRequest("abcddfad", TestRequest.LOOKUP));
//
//      testRequest.add(new TestRequest("qwerty", TestRequest.UPDATE));
//
//      testRequest.add(new TestRequest(name, TestRequest.REMOVE));
//
//      testRequest.add(new TestRequest(name, TestRequest.REMOVE));
//
//      testRequest.add(new TestRequest(name, TestRequest.REMOVE));

      // todo is there a way to test responses automatically? e.g., do a lookup after update to test if it works.

      double mean = 5000.0;
      Constant constantValue = new Constant(mean);
      new RequestGenerator().generateRequests(new WorkloadParams(null), testRequest, constantValue,
              LocalNameServer.getRequestHandler());

    } catch (IOException e) {
      GNS.getLogger().info("ERROR: Test unsuccessful due to exception.");
      e.printStackTrace();
      System.exit(2);
    }

  }


  /**
   * Recursively deletes the given file/directory
   * @param f
   */
  private static void deleteDir(File f) {
    if (f.exists() == false) {
      return;
    }
    if (f.isFile()) {
      f.delete();
      return;
    }

    File[] f1 = f.listFiles();
    for (File f2 : f1) {
      if (f2.isFile()) {
        f2.delete();
      } else if (f2.isDirectory()) {
        deleteDir(f2);
      }
    }
    f.delete();
  }

}
