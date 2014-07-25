package edu.umass.cs.gns.test;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.GNSNodeConfig;
import edu.umass.cs.gns.nsdesign.NameServer;

import java.io.IOException;
import java.util.HashMap;

/**
 * Test to show that a name server can be started and stopped without exceptions getting printed.
 * Created by abhigyan on 7/24/14.
 */
public class NameServerStartStopTest {


  public static void main(String[] args) throws IOException, InterruptedException {
    // NOTE: Mongo DB must be running before running this test.
    GNSNodeConfig gnsNodeConfig = new GNSNodeConfig(3, 1, 0);
    HashMap<String, String> params = null;
    // start name server a few times
    for (int i = 0; i < 5; i ++) {
      GNS.getLogger().info("\n\n\n\nStarting Name Server for " + (i+i) + "-th time.\n\n");
      NameServer ns = new NameServer(0, params, gnsNodeConfig);
      Thread.sleep(5000);
      ns.shutdown();
    }
    System.out.println("Name server started and stopped multiple times without generating exceptions.");
  }
}
