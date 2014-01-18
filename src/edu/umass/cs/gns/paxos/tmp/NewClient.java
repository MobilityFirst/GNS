package edu.umass.cs.gns.paxos.tmp;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartNameServer;
import edu.umass.cs.gns.nio.ByteStreamToJSONObjects;
import edu.umass.cs.gns.nio.NioServer;
import edu.umass.cs.gns.nio.NodeConfig;
import edu.umass.cs.gns.nio.PacketDemultiplexer;
import edu.umass.cs.gns.packet.paxospacket.PaxosPacketType;
import edu.umass.cs.gns.packet.paxospacket.RequestPacket;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Random;
import java.util.TimerTask;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created with IntelliJ IDEA.
 * User: abhigyan
 * Date: 6/29/13
 * Time: 11:34 PM
 * To change this template use File | Settings | File Templates.
 */
public class NewClient  extends PacketDemultiplexer{

  /**
   * Non-blocking tcp connection object
   */
  NioServer nioServer;

  /**
   * Timer object to send requests at given rate
   */
  ScheduledThreadPoolExecutor t = new ScheduledThreadPoolExecutor(4);

  /**
   * ID of this client
   */
  int ID;

  /**
   * config file listing address, port of paxos replicas, and clients
   */
  String nodeConfigFile;

  /**
   * config file for generating workload at client
   */
  String testConfig;

  int numberOfReplicas = 5;
  /**
   * number of requests sent
   */
  int numberRequests = 100;

  /**
   * test duration in seconds
   */
  int testDurationSeconds = 10;

  /**
   * nodeID of server replica to which this client sends requests
   */
  int defaultReplica = 0;

  /**
   * ID of the paxos instance creating during testing.
   */
  String defaultPaxosID = PaxosManager.defaultPaxosID;

  /**
   * number of responses received
   */
  int responseCount = 0;

  public static int groupsize = 1;

  public static int printsize = 1000;

  //    /**
//     * lock for the {@code responseCount} field
//     */
  ReentrantLock lock = new ReentrantLock();

  FileWriter outputWriter = null;

  private static Random rand = new Random();

  private String outputFolder;
  /**
   *
   * @param ID
   * @param nodeConfigFile
   * @param testConfig
   */
  public NewClient(int ID, String nodeConfigFile, String testConfig) {
    this.ID = ID;
    this.nodeConfigFile = nodeConfigFile;
    this.testConfig = testConfig;

  }

  /**
   * Test paxos implementation by sending requests to replicas
   */
  public void testPaxos() {

    // start TCP socket
    initTransport();

    // start to send requests as per testConfig
    startSendingRequests();
  }

  /**
   * Initialize transport object to send/receive objects
   */
  private void initTransport() {

    try {
      System.out.println(" ID is " + ID);
      NodeConfig nodeConfig = new PaxosNodeConfig(nodeConfigFile);
      InetAddress add = nodeConfig.getNodeAddress(ID);
      System.out.println(" Address  is " + add);
      nioServer = new NioServer(ID, new ByteStreamToJSONObjects(this), nodeConfig);
    } catch (IOException e) {
      GNS.getLogger().severe(" Could not initialize TCP socket at client");
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      return;
    }
    new Thread(nioServer).start();
  }

  /**
   * Start to send requests
   */
  private void startSendingRequests() {
    readTestConfig();

    System.out.println(" Client " + ID + " starting to send requests ..");

    double interRequestIntervalMilliseconds = testDurationSeconds*1000.0/numberRequests* NewClient.groupsize;
    double delay = 0;
//        long t0 = System.currentTimeMillis();
    for (int i = 1; i <= numberRequests/ NewClient.groupsize; i++) {
//            int replica = i % numberOfReplicas;
      int replica = defaultReplica;
      boolean x = false;
      if (i == numberRequests/ NewClient.groupsize) x = true;
      SendRequestTask task = new SendRequestTask(i, ID, defaultPaxosID, replica, nioServer, x);
      t.schedule(task, (long)delay, TimeUnit.MILLISECONDS);
      delay += interRequestIntervalMilliseconds;
//            if (delay > 10000) {
//                if (StartNameServer.debugMode) GNS.getLogger().severe(" Requests scheduled = " + i  + " delay = " + delay);
//                try{
//                    long sub = System.currentTimeMillis() - t0;
//                    t0 = System.currentTimeMillis();
//                    Thread.sleep((long) delay - sub);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//                }
//                delay = 0;
//            }
//            System.out.println(" Sent request " + i + " ");
    }

    try {
      Thread.sleep(this.testDurationSeconds*1000);
      long extraWaitMilliseconds = 2000;
      Thread.sleep(extraWaitMilliseconds);
    } catch (InterruptedException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }

    try {
      lock.lock();
      double latency = LatencyCalculator.getAverageLatency();
      LatencyCalculator.resetCalculation();
      writeKeyValueOutput("RequestSent", Integer.toString(numberRequests));
      writeKeyValueOutput("ResponseReceived", Integer.toString(responseCount));
      writeKeyValueOutput("Failure", Double.toString((numberRequests - responseCount)*100/numberRequests));
      System.out.println(" Requests sent = " + numberRequests + " Response received = " + responseCount + " Avg Latency = " + latency);
    } finally {
      lock.unlock();
    }
    closeOutputWriter();
    System.out.println(" Client is quitting. Client ID = " + ID  );
    System.exit(2);
  }

  private void closeOutputWriter() {
    try {
      outputWriter.close();
    } catch (IOException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
  }
  private void writeKeyValueOutput(String key, String value) {
    if (outputWriter!= null) {
      try {
        outputWriter.write(key + "\t" + value + "\n");
      } catch (IOException e) {
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }
    }
  }
  /**
   * client received paxos decision (server echoes back the request it sent)
   */
  public void handlePaxosDecision() {
    try{
      lock.lock();
      responseCount ++;
      if (responseCount%printsize == 0) {
        double latency = LatencyCalculator.getAverageLatency();
        LatencyCalculator.resetCalculation();
        writeKeyValueOutput("Latency", Double.toString(latency));
        System.out.println(" Received response " + responseCount + "\tAvg Latency = " + latency);
      }
      LatencyCalculator.addResponseReceivedTime();
    }finally{
      lock.unlock();
    }
  }


  public static String getRandomString() {
    int intRange = 1000000;
    Integer x = intRange + rand.nextInt(1000000);
    return x.toString();
  }

  private void readTestConfig() {
    File f = new File(testConfig);
    if (!f.exists()) {
      System.out.println(" testConfig file does not exist. Quit. " +
              "Filename =  " + testConfig);
      System.exit(2);
    }

    try {
      BufferedReader br = new BufferedReader(new FileReader(f));
      while (true) {
        String line = br.readLine();
        if (line == null) break;
        String[] tokens = line.trim().split("\\s+");
        if (tokens.length != 2) continue;
        if (tokens[0].equals("NumberOfRequests")) {
          numberRequests = Integer.parseInt(tokens[1]);
          printsize = (numberRequests/10 == 0) ? 1 : numberRequests/10;
        }
        else if (tokens[0].equals("TestDurationSeconds")) testDurationSeconds = Integer.parseInt(tokens[1]);
        else if (tokens[0].equals("NumberOfReplicas")) numberOfReplicas = Integer.parseInt(tokens[1]);
        else if (tokens[0].equals("OutputFolder"))  {
          outputFolder = tokens[1];
          initOutputWriter();


        }
      }
    } catch (IOException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
  }

  public void initOutputWriter() {
    if (outputFolder == null) {
      System.out.println(" Output Folder is NULL.");
      return;
    }
    File f = new File(outputFolder);
    if (f.exists() == false) {
      f.mkdirs();
    }
    try {
      outputWriter = new FileWriter(outputFolder + "/paxos_output");
    } catch (IOException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
  }
  @Override
  public void handleJSONObjects(ArrayList jsonObjects) {
    for (Object j: jsonObjects) {
      handlePaxosDecision();
    }
  }



  /**
   *
   * @param args
   */
  public static void main(String[] args){
    StartNameServer.debugMode  = false;
    if (args.length != 3) {
      System.out.println("Exiting Client.\nUsage: NewClient  <clientID> <nodeConfigFile> <testConfigFile>");
      System.exit(2);
    }
    String nodeConfigFile = args[0];
    String testConfig = args[1];
    String sID = args[2];
    NewClient client = new NewClient(Integer.parseInt(sID), nodeConfigFile, testConfig);
    client.testPaxos();

  }


}


class SendRequestTask extends TimerTask {

  int requestID;

  int ID;

  String defaultPaxosID;

  int replica;

  NioServer nioServer;

  boolean stop;

  public SendRequestTask(int requestID, int ID, String defaultPaxosID,
                         int defaultReplica, NioServer nioServer, boolean stop) {
    this.requestID = requestID;
    this.ID = ID;
    this.defaultPaxosID = defaultPaxosID;
    this.replica = defaultReplica;
    this.nioServer = nioServer;
    this.stop = stop;
  }

  @Override
  public void run() {

    for (int i = 0; i < NewClient.groupsize; i++) {
      try {
        boolean x = false;
//              if (stop && i == NewClient.groupsize - 1) {
//                x = true;
//              }
        if (x) System.out.println(" Stop request = " + x);

        RequestPacket requestPacket = new RequestPacket(ID, NewClient.getRandomString(), PaxosPacketType.REQUEST, x);
//              System.out.println(" request = " + requestPacket);
        JSONObject json = requestPacket.toJSONObject();
        json.put(PaxosManager.PAXOS_ID, defaultPaxosID); // send request to paxos instance with ID = 0.
        nioServer.sendToID(replica, json);
        LatencyCalculator.addRequestSendTime();
//                System.out.println(" XXXXXXXXXSent " + requestPacket + " to " + replica);
      } catch (IOException e) {
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      } catch (JSONException e) {
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }
    }

    if ((requestID* NewClient.groupsize)% NewClient.printsize == 0) {
      System.out.println(" Sent request " + (requestID* NewClient.groupsize) + " ");
    }
  }


}


class LatencyCalculator {

  private static long totalLatency = 0;
  private static  int requestCount = 0;

  private static ArrayList<Long> requestSendTimes = new ArrayList<Long>();

  static synchronized void  addRequestSendTime() {
    requestSendTimes.add(System.currentTimeMillis());
    requestCount++;
  }

  static synchronized void addResponseReceivedTime() {
    if (requestSendTimes.size() == 0) {
      System.out.println(" ERROR: More responses received than request sent: " + requestCount);
      return;
    }
    long sendTime = requestSendTimes.remove(0);
    totalLatency += System.currentTimeMillis() - sendTime;

  }

  static synchronized double getAverageLatency() {
    if (requestCount == 0) return 0;
    return totalLatency*1.0/requestCount;

  }

  static synchronized void resetCalculation() {
    totalLatency = 0;
    requestCount = 0;
//    requestSendTimes.clear();
  }
}