package edu.umass.cs.gns.test.connecttime;

import edu.umass.cs.gns.clientsupport.Intercessor;
import edu.umass.cs.gns.clientsupport.QueryResult;
import edu.umass.cs.gns.localnameserver.LocalNameServer;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.util.NameRecordKey;
import edu.umass.cs.gns.workloads.ExponentialDistribution;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.Socket;
import java.util.TimerTask;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Represents the correspondent attempting to connect to a mobile. Its reads current value of mobile's address from
 * the system, and then verifies with the mobile whether the address is correct. Otherwise,
 *
 * Created by abhigyan on 5/13/14.
 */
public class Correspondent implements Runnable{

  private ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(10);

  private double lookupIntervalSec = 0.1;  // this should be configurable

  private double maxExpDurationSec = 3600.0;  // this should be configurable

  private int timeoutSec = 5;

  private int maxNumTimeout = 5;

  private InetAddress mobileAddress;

  private int mobilePort = 23498;

  private String name = "test_name";

  private String key = NameRecordKey.EdgeRecord.getName();

  public Correspondent(int mobileID) {
    mobileAddress = LocalNameServer.getGnsNodeConfig().getNodeAddress(mobileID);
  }

  @Override
  public void run() {
    long t0 = System.currentTimeMillis();
    ExponentialDistribution expDistribution = new ExponentialDistribution(lookupIntervalSec);
    while (true) {
      // read and connect
      if (System.currentTimeMillis() - t0 > maxExpDurationSec*1000) {
        GNS.getLogger().info("Experiment over: duration = " + (System.currentTimeMillis() - t0)/1000 + " sec");
        break;
      }
      try {
        Thread.sleep((int)(expDistribution.getNextArrivalDelay()*1000));
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      ReadTask read = new ReadTask();
      executor.submit(read);
    }
  }

  class ReadTask extends TimerTask {
    long connectTime = 0;
    int numTimeout = 0;

    public ReadTask() {
    }

    public ReadTask(long connectTime, int numTimeout) {
      this.connectTime = connectTime;
      this.numTimeout = numTimeout;
    }

    @Override
    public void run() {
      // 1. read a value from GNS
      // 2. connect to mobile and verify name
      // 3. if does not match, sleep and repeat.

      long t0 = System.currentTimeMillis();
      QueryResult result = Intercessor.sendQueryBypassingAuthentication(name, key);
      connectTime += System.currentTimeMillis() - t0;

      String valueFromGns = null;
      if (result != null && result.getArray(key) != null && result.getArray(key).size() > 0) {
        valueFromGns = (String) result.getArray(key).get(0);
      }

      t0 = System.currentTimeMillis();
      String valueFromMobile = readCurrentValueFromMobile();
      long valueReadTime = System.currentTimeMillis() - t0;

      GNS.getLogger().fine("Value from GNS = " + valueFromGns + " value from mobile = " + valueFromMobile);
      if (valueFromGns == null || valueFromMobile == null) {
        GNS.getLogger().warning("Null Value/s: value from GNS = " + valueFromGns + " value from mobile = " + valueFromMobile);
      }

      if (valueFromGns != null && valueFromMobile != null && valueFromMobile.equals(valueFromGns)) {
        GNS.getStatLogger().info("\tSuccess-ConnectTime\t" + connectTime + "\t");
      } else if (numTimeout + 1 == maxNumTimeout){
        GNS.getStatLogger().info("\tFailed-ConnectTime\t" + (connectTime + timeoutSec*1000) + "\t");
      } else {
        long delay = Math.max(timeoutSec * 1000 - valueReadTime, 0);
        executor.schedule(new ReadTask(connectTime + timeoutSec*1000, numTimeout + 1), delay, TimeUnit.MILLISECONDS);
      }
    }

    /**
     * Connects to mobile and reads current value.
     * @return Either value returned by mobile or NULL in case of IOException
     */
    private String readCurrentValueFromMobile() {
      String line = null;
      try {
        Socket clientSocket = new Socket(mobileAddress, mobilePort);
        BufferedReader inFromServer = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        line = inFromServer.readLine();
        clientSocket.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
      return line;
    }

  }
}
