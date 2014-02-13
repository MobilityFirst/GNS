package edu.umass.cs.gns.paxos;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nio.ByteStreamToJSONObjects;
import edu.umass.cs.gns.nio.NioServer;
import edu.umass.cs.gns.nio.NodeConfig;
import edu.umass.cs.gns.nio.PacketDemultiplexer;
import edu.umass.cs.gns.packet.paxospacket.PaxosPacketType;
import edu.umass.cs.gns.packet.paxospacket.RequestPacket;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
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
//  String nodeConfigFile;

  /**
   *
   */
  TestConfig testConfig1;


//  int numberOfReplicas = 5;
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
   * @param testConfig
   */
  public NewClient(int ID, String testConfig) {
    this.ID = ID;
//    this.nodeConfigFile = nodeConfigFile;
//    this.testConfig = testConfig;
    readTestConfig(testConfig);
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
//      System.out.println(" ID is " + ID);
      NodeConfig nodeConfig = new PaxosNodeConfig(testConfig1.numPaxosReplicas + 1, testConfig1.startingPort);
      InetAddress add = nodeConfig.getNodeAddress(ID);
//      System.out.println(" Address  is " + add);
      nioServer = new NioServer(ID, new ByteStreamToJSONObjects(this), nodeConfig);
    } catch (IOException e) {
      GNS.getLogger().severe(" Could not initialize TCP socket at client");
      e.printStackTrace();  
      return;
    }
    new Thread(nioServer).start();
  }

  /**
   * Start to send requests
   */
  private void startSendingRequests() {

    initOutputWriter();

    System.out.println(" Client " + ID + " starting to send requests ..");

    double interRequestIntervalMilliseconds = testDurationSeconds*1000.0/numberRequests*NewClient.groupsize;
    double delay = 0;

    for (int i = 1; i <= numberRequests/NewClient.groupsize; i++) {

      int replica = defaultReplica;
      boolean x = false;
      if (i == numberRequests/NewClient.groupsize) x = true;
      SendRequestTask task = new SendRequestTask(i, ID, testConfig1.testPaxosID, replica, nioServer, x);
      t.schedule(task, (long)delay, TimeUnit.MILLISECONDS);
      delay += interRequestIntervalMilliseconds;
    }

    try {
      Thread.sleep(this.testDurationSeconds*1000);
      long extraWaitMilliseconds = 2000;
      Thread.sleep(extraWaitMilliseconds);
    } catch (InterruptedException e) {
      e.printStackTrace();  
    }


    try {
      lock.lock();
      double latency = LatencyCalculator.getAverageLatency();
      LatencyCalculator.resetCalculation();
      writeKeyValueOutput("RequestSent", Integer.toString(numberRequests));
      writeKeyValueOutput("ResponseReceived", Integer.toString(responseCount));
      writeKeyValueOutput("Failure", Double.toString((numberRequests - responseCount)*100/numberRequests));
      System.out.println(" Requests sent = " + numberRequests + " Response received = " + responseCount + " Avg Latency = " + latency);
      if (numberRequests == responseCount) {
        System.out.println("\n***Test SUCCESS***\n");
      } else {
        System.out.println("\n***Test FAILED: requests and responses are not equal***\n");
      }
    } finally {
      lock.unlock();
    }
    closeOutputWriter();

    System.out.println("Output written to file: " + outputFolder + "/paxos_output");

    System.out.println(" Client exit. Client ID = " + ID  );
    System.exit(2);
  }

  private void closeOutputWriter() {
    try {
      outputWriter.close();
    } catch (IOException e) {
      e.printStackTrace();  
    }
  }
  private void writeKeyValueOutput(String key, String value) {
    System.out.println(key + "\t" + value);
    if (outputWriter!= null) {
      try {
        outputWriter.write(key + "\t" + value + "\n");
      } catch (IOException e) {
        e.printStackTrace();  
      }
    }
  }
  /**
   * client received paxos decision (server echoes back the request it sent)
   */
  public void handlePaxosDecision(JSONObject jsonObject) {
    try{
      lock.lock();
      responseCount ++;
      if (responseCount%printsize == 0) {
        double latency = LatencyCalculator.getAverageLatency();
//        LatencyCalculator.resetCalculation();
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


  private void readTestConfig(String testConfigFile) {

    this.testConfig1 = new TestConfig(testConfigFile);
//    numberOfReplicas = testConfig1.numPaxosReplicas;
    numberRequests = testConfig1.numberRequests;
    printsize = (numberRequests/10 == 0) ? 1 : numberRequests/10;
//    testPaxosID = testConfig1.testPaxosID;
    testDurationSeconds = testConfig1.testDurationSeconds;

    outputFolder = testConfig1.outputFolder;




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
      e.printStackTrace();  
    }
  }
  @Override
  public void handleJSONObjects(ArrayList jsonObjects) {
    for (Object j: jsonObjects) {
      handlePaxosDecision((JSONObject) j);
    }
  }


  /**
   *
   * @param args
   */
  public static void main(String[] args){

    /**
     *
     Paxos module can be tested by running this java file: NewClient.java.

     The current test sends a given  number of requests from a client to one of the paxos replicas.

     All paxos replicas agree on the ordering of the request and return the same request back to client.

     At the end, the test outputs the number of requests sent, number of responses received, and the average latency of requests.

     The test requires a config file, a sample config file is located at resources/testCodeResources/testConfig.

     The sample file explains the parameters you can vary in running the test.
     */

//    String nodeConfigFile = "resources/testCodeResources/nodeConfig"; //args[0];
    String testConfigFile = "resources/testCodeResources/testConfig";//args[1];

    TestConfig testConfig1 = new TestConfig(testConfigFile);

    // create paxos instances
    for (int i = 0; i < testConfig1.numPaxosReplicas; i++) {
//      System.out.println("started paxos " + i);
      new PaxosManager(testConfigFile, i);
    }
    try {
      Thread.sleep(10);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    int clientID = testConfig1.numPaxosReplicas;
    // create paxos
    NewClient client = new NewClient(clientID,  testConfigFile);
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

      try {

        boolean x = false;
        if (x) System.out.println(" Stop request = " + x);

        RequestPacket requestPacket = new RequestPacket(ID, NewClient.getRandomString(), PaxosPacketType.REQUEST, x);
        JSONObject json = requestPacket.toJSONObject();
        json.put(PaxosManager.PAXOS_ID, defaultPaxosID); // send request to paxos instance with ID = 0.
        nioServer.sendToID(replica, json);
        LatencyCalculator.addRequestSendTime();
      } catch (IOException e) {
        e.printStackTrace();  
      } catch (JSONException e) {
        e.printStackTrace();  
      }

    if ((requestID)%NewClient.printsize == 0) {
      System.out.println(" Sent request " + (requestID*NewClient.groupsize) + " ");
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