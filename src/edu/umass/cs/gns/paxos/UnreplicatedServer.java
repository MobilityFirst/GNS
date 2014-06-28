package edu.umass.cs.gns.paxos;

import edu.umass.cs.gns.main.GNS;
//import edu.umass.cs.gns.main.StartNameServer;
import edu.umass.cs.gns.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.gns.nio.deprecated.ByteStreamToJSONObjects;
import edu.umass.cs.gns.nio.deprecated.NioServer;
import edu.umass.cs.gns.nsdesign.Config;
import org.json.JSONObject;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.TimerTask;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created with IntelliJ IDEA. User: abhigyan Date: 7/21/13 Time: 6:54 AM To change this template use File | Settings | File
 * Templates.
 */
public class UnreplicatedServer {

  static int nodeID;
//    static int N;
  public static ScheduledThreadPoolExecutor executorService;
  private static String serverLogFileName;
  /**
   * When paxos is run independently {@code tcpTransport} is used to send messages between paxos replicas and client.
   */
  public static NioServer tcpTransport;

  public static void initializeUnreplicatedServer(int nodeID1, String nodeConfigFile, String serverLogFileName1) {
    nodeID = nodeID1;
    File f = new File(serverLogFileName1);
    if (f.exists()) {
      f.delete();
    }
    serverLogFileName = serverLogFileName1;
    initExecutor();
    initTransport(nodeConfigFile);
  }

  /**
   * initialize executor service during Paxos debugging/testing
   */
  private static void initExecutor() {
    int maxThreads = 4;
    executorService = new ScheduledThreadPoolExecutor(maxThreads);
  }

  /**
   * initialize transport object during Paxos debugging/testing
   *
   * @param nodeConfigFile config file containing list of node ID, IP, port
   */
  private static void initTransport(String nodeConfigFile) {

    // create the worker object
    SimplePacketDemultiplexer simpleDemux = new SimplePacketDemultiplexer();
    ByteStreamToJSONObjects worker = new ByteStreamToJSONObjects(simpleDemux);

    // earlier worker was running as a separate thread
//        new Thread(worker).start();

    // start TCP transport thread
    try {
      tcpTransport = new NioServer(nodeID, worker, new PaxosNodeConfig(nodeConfigFile));
      if (Config.debugMode) {
        GNS.getLogger().fine(" TRANSPORT OBJECT CREATED ... ");
      }
      new Thread(tcpTransport).start();
    } catch (IOException e) {
      GNS.getLogger().severe(" Could not initialize TCP socket at client");
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
  }
  public static final ReentrantLock REENTRANT_LOCK = new ReentrantLock();

  public static void logDecision(int sequence, String decision) {
    try {
      FileWriter fw = new FileWriter(serverLogFileName, true);
      fw.write(sequence + "\t" + decision + "\n");
      fw.close();
    } catch (IOException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }

  }
  private static int requestNumber = 0;

  public static int getRequestNumber() {
    return ++requestNumber;
  }

  public static void main(String[] args) {

    Config.debugMode = false;
    String nodeConfigFile = args[0];
    String serverLogFileName = args[1];
    int nodeID = Integer.parseInt(args[2]);
    initializeUnreplicatedServer(nodeID, nodeConfigFile, serverLogFileName);
  }
}

/**
 * {@code tcpTransport} in {@code UnreplicatedServer} will forward received messages to this class.
 */
class SimplePacketDemultiplexer extends AbstractPacketDemultiplexer {


  public boolean handleJSONObject(JSONObject jsonObject) {
    UnreplicatedSendClientResponseTask task = new UnreplicatedSendClientResponseTask(jsonObject);
    // run the task to handle client request
    UnreplicatedServer.executorService.submit(task);
    return true;
//            try {
//                UnreplicatedServer.tcpTransport.sendToID(requestPacket.clientID, jsonObject);
//                System.out.println("Sent to client ...");
//            } catch (IOException e) {
//                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//            }
//            JSONObject jsonObject = (JSONObject) o;
//            RequestPacket requestPacket = null;
//            try {
//                requestPacket = new RequestPacket(jsonObject);
//            } catch (JSONException e) {
//                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//                return;
//            }
      //
//            synchronized (UnreplicatedServer.REENTRANT_LOCK) {
//            int reqNumber = UnreplicatedServer.getRequestNumber();
//            UnreplicatedServer.logDecision(reqNumber,requestPacket.value);
//            UnreplicatedServer.logDecision(reqNumber,requestPacket.value);
//
////            }
//            try {
//                UnreplicatedServer.tcpTransport.sendToID(requestPacket.clientID, requestPacket.toJSONObject());
//            } catch (IOException e) {
//                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//            } catch (JSONException e) {
//                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//            }

  }
}

/**
 *
 */
class UnreplicatedSendClientResponseTask extends TimerTask {

  JSONObject jsonObject;
//    RequestPacket requestPacket;

  public UnreplicatedSendClientResponseTask(JSONObject jsonObject1) {
    this.jsonObject = jsonObject1;
  }

  @Override
  public void run() {

//        try {
//            requestPacket = new RequestPacket(jsonObject);
//        } catch (JSONException e) {
//            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//            return;
//        }
    //
//        synchronized (UnreplicatedServer.REENTRANT_LOCK) {
//            int reqNumber = UnreplicatedServer.getRequestNumber();
////            UnreplicatedServer.logDecision(reqNumber,requestPacket.value);
////            UnreplicatedServer.logDecision(reqNumber,requestPacket.value);
//        }
    try {
      UnreplicatedServer.tcpTransport.sendToID(1, jsonObject);
      System.out.println("Sent to client ...");
    } catch (IOException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }

  }
}