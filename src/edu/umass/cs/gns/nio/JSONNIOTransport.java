package edu.umass.cs.gns.nio;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.nio.nioutils.NIOInstrumenter;
import edu.umass.cs.gns.nio.nioutils.PacketDemultiplexerDefault;
import edu.umass.cs.gns.nio.nioutils.SampleNodeConfig;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author V. Arun
 */

/*
 * Consider using JSONMessenger any place you want to use this class. The
 * former subsumes this class and also has support for MessagingTask objects.
 * 
 * This class exists primarily as a GNS wrapper around NIOTransport. NIOTransport
 * is for general-purpose NIO byte stream communication between numbered nodes as
 * specified by the NodeConfig interface and a data processing worker as specified
 * by the DataProcessingWorker interface.
 * 
 * The GNS-specific functions include (1) delay emulation, (2) short-circuiting
 * local sends by directly sending it to packetDemux, and (3) adding GNS-specific
 * headers to NIO's byte stream abstraction, and (4) supporting a crazy number of
 * redundant public methods to do different kinds of sends. These methods exist
 * only for backwards compatibility.
 */
public class JSONNIOTransport<NodeIDType> extends NIOTransport<NodeIDType> implements
        InterfaceJSONNIOTransport<NodeIDType> {

  public static final String DEFAULT_IP_FIELD = "_IP_ADDRESS1";
  // didn't add this to "stamping" code below but it is declared for other uses
  public static final String DEFAULT_PORT_FIELD = "_TCP_PORT";

  private String IPField = null;
  private String portField = null;

  public static final boolean DEBUG = false; // enables send monitoring

  public JSONNIOTransport(NodeIDType id, InterfaceNodeConfig<NodeIDType> nodeConfig)
          throws IOException {
    super(id, nodeConfig, new JSONMessageExtractor()); // Note: Default extractor will not do any useful demultiplexing
  }

  public JSONNIOTransport(NodeIDType id, InterfaceNodeConfig<NodeIDType> nodeConfig,
          JSONMessageExtractor worker) throws IOException {
    super(id, nodeConfig, worker); // Switched order of the latter two arguments
  }

  // common case usage, hence created a constructor
  public JSONNIOTransport(NodeIDType id, InterfaceNodeConfig<NodeIDType> nodeConfig,
          AbstractPacketDemultiplexer pd, boolean start) throws IOException {
    super(id, nodeConfig, new JSONMessageExtractor(pd)); // Switched order of the latter two arguments
    //addPacketDemultiplexer(pd);
    if (start && !isStarted()) {
      (new Thread(this)).start();
    }
  }

  public void addPacketDemultiplexer(AbstractPacketDemultiplexer pd) {
    ((JSONMessageExtractor) this.worker).addPacketDemultiplexer(pd);
  }

  @Override
  public NodeIDType getMyID() {
    return this.myID;
  }

  public void stop() {
    super.stop();
    ((JSONMessageExtractor) this.worker).stop();
    JSONDelayEmulator.stop();
  }

  /**
   * ******************Start of send methods****************************************
   */

  /*
   * Note: Delay emulation is currently implemented only for ID-based sends, not
   * IP based sends. Even for ID-based sends, it is incorrect as the return
   * value is not meaningful. It is used only for testing if at all.
   */
  /**
   * Send a JSON packet to an NameServer using the id.
   *
   * @param id
   * @param jsonData
   * @return
   * @throws IOException
   */
  @Override
  public int sendToID(NodeIDType id, JSONObject jsonData) throws IOException {
    if (JSONDelayEmulator.isDelayEmulated()) {
      JSONDelayEmulator.putEmulatedDelay(id, jsonData);
    }
    return sendToIDActual(id, jsonData);
  }

  /**
   * Send a JSON packet to an inet socket address (ip and port).
   *
   * @param isa
   * @param jsonData
   * @return
   * @throws IOException
   */
  @Override
  public int sendToAddress(InetSocketAddress isa, JSONObject jsonData)
          throws IOException {
    int written = 0;
    int originalSize = jsonData.toString().length();
    //stampSenderInfo(jsonData);
    String headeredMsg = JSONMessageExtractor.prependHeader(jsonData.toString());
    written = this.sendUnderlying(isa, headeredMsg.getBytes())
            - (headeredMsg.length() - originalSize); // subtract header length
    assert (written < 0 || written == originalSize);
    return written;
  }

  /*
   * This method adds a header only if a socket channel is used to send to
   * a remote node, otherwise it hands over the message directly to the worker.
   */
  protected int sendToIDActual(NodeIDType destID, JSONObject jsonData)
		  throws IOException {
	  int written = 0;
	  int originalSize = jsonData.toString().length();
	  //stampSenderInfo(jsonData);
	  if (destID.equals(this.myID)) {
		  sendLocal(jsonData); 
		  written = originalSize; // local send just passes pointers
	  } else {
		  String headeredMsg = JSONMessageExtractor.prependHeader(jsonData.toString());
		  written = (this.sendUnderlying(destID, headeredMsg.getBytes())
				  - (headeredMsg.length() - originalSize)); // subtract header length
	  }
	  assert(written < 0 || written == originalSize) : written + " != " + originalSize;
	  return written;
  }

  public JSONNIOTransport<NodeIDType> enableStampSenderInfo() {
    this.IPField = DEFAULT_IP_FIELD;
    this.portField = DEFAULT_PORT_FIELD;
    return this;
  }

  public JSONNIOTransport<NodeIDType> enableStampSenderPort() {
    this.portField = DEFAULT_PORT_FIELD;
    return this;
  }

  public JSONNIOTransport<NodeIDType> disableStampSenderIP() {
    this.IPField = null;
    this.portField = null;
    return this;
  }

  public static InetSocketAddress getSenderAddress(JSONObject json) {
    try {
      InetAddress address = (json.has(JSONNIOTransport.DEFAULT_IP_FIELD)
              ? InetAddress.getByName(json.getString(JSONNIOTransport.DEFAULT_IP_FIELD).replaceAll("[^0-9.]*", "")) : null);
      int port = (json.has(JSONNIOTransport.DEFAULT_PORT_FIELD)
              ? json.getInt(JSONNIOTransport.DEFAULT_PORT_FIELD) : -1);
      if (address != null && port > 0) {
        return new InetSocketAddress(address, port);
      }
    } catch (UnknownHostException | JSONException e) {
      e.printStackTrace();
    }
    return null;
  }

  public static String getSenderInetAddressAsString(JSONObject json) {
    try {
      String address = (json.has(JSONNIOTransport.DEFAULT_IP_FIELD)
              ? (json.getString(JSONNIOTransport.DEFAULT_IP_FIELD).replaceAll("[^0-9.]*", ""))
              : null);
      return address;
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return null;
  }

  public static int getSenderPort(JSONObject json) {
    try {
      int port = (json.has(JSONNIOTransport.DEFAULT_PORT_FIELD)
              ? (json.getInt(JSONNIOTransport.DEFAULT_PORT_FIELD))
              : -1);
      return port;
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return -1;
  }

  /**
   * ******************End of public send methods************************************
   */
  // FIXME: This method is not used and should be used or removed
  // stamp sender IP address if IPField is set
  protected void stampSenderInfo(JSONObject jsonData) {
    try {
      if (this.IPField != null && !this.IPField.isEmpty()) {
        jsonData.put(this.IPField, this.getNodeAddress().toString());
      }
      if (this.portField != null && !this.portField.isEmpty()) {
        jsonData.put(this.portField, this.getNodePort());
      }

    } catch (JSONException je) {
      je.printStackTrace();
    }
  }

  // fake send by directly passing to local worker
  private int sendLocal(JSONObject jsonData) {
	  /* We create a deep copy for local sends as 
	   * otherwise it can end up getting modified
	   * by the receiver end and cause the number
	   * of bytes written to be not equal to those
	   * sent for sends to receivers other than
	   * the sender when the same message is being
	   * sent to a set of nodes including self. 
	   */
	  try {
		  jsonData = new JSONObject(jsonData.toString());
	  } catch (JSONException e) {
		  e.printStackTrace();
		  return -1;
	  }

	  int length = jsonData.toString().length();
	  ArrayList<JSONObject> jsonArray = new ArrayList<JSONObject>();
	  jsonArray.add(jsonData);
	  NIOInstrumenter.incrSent();
	  ((JSONMessageExtractor) worker).processJSONMessages(jsonArray);
	  /* Note: Can not return jsonData.toString().length() as it
	   * may be changed at the receiving end. Local sends are 
	   * just passing pointers, not actually using networking.
	   */
	  return length;
  }

  /**
   * *************************** Start of calls to NIO send *************************
   */
  /*
   * These methods are really redundant. But they exist so that there is one place where
   * all NIO sends actually happen. Do NOT add more gunk to this method.
   */
  private int sendUnderlying(NodeIDType id, byte[] data) throws IOException {
    if (DEBUG) log.info("Send to: " + id + " json: " + new String(data));
    return this.send(id, data);
  }

  private int sendUnderlying(InetSocketAddress isa, byte[] data)
          throws IOException {
    return this.send(isa, data);
  }

  /**
   * *************************** End of calls to NIO send *************************
   */
  private static JSONObject JSONify(int msgNum, String s)
          throws JSONException {
    return new JSONObject("{\"msg\" : \"" + s + "\" , \"msgNum\" : " + msgNum + "}");
  }

  /*
   * The test code here is mostly identical to that of NIOTransport but tests
   * JSON messages, headers, and delay emulation features. Need to test it with
   * the rest of GNS.
   */
  @SuppressWarnings("unchecked")
  public static void main(String[] args) {
    int msgNum = 0;
    int port = 2000;
    int nNodes = 100;
    SampleNodeConfig<Integer> snc = new SampleNodeConfig<Integer>(port);
    snc.localSetup(nNodes + 2);
    JSONMessageExtractor[] workers = new JSONMessageExtractor[nNodes + 1];
    for (int i = 0; i < nNodes + 1; i++) {
      workers[i]
              = new JSONMessageExtractor(new PacketDemultiplexerDefault());
    }
    JSONNIOTransport<?>[] niots = new JSONNIOTransport[nNodes];

    try {
      int smallNNodes = 2;
      for (int i = 0; i < smallNNodes; i++) {
        niots[i] = new JSONNIOTransport<Integer>(i, snc, workers[i]);
        new Thread(niots[i]).start();
      }

      /**
       * **********************************************************************
       */
      /*
       * Test a few simple hellos. The sleep is there to test
       * that the successive writes do not "accidentally" benefit
       * from concurrency, i.e., to check that OP_WRITE flags will
       * be set correctly.
       */
      ((JSONNIOTransport<Integer>) niots[1]).sendToIDActual(0, JSONify(msgNum++, "Hello from 1 to 0"));
      ((JSONNIOTransport<Integer>) niots[0]).sendToIDActual(1,
              JSONify(msgNum++, "Hello back from 0 to 1"));
      ((JSONNIOTransport<Integer>) niots[0]).sendToIDActual(1,
              JSONify(msgNum++, "Second hello back from 0 to 1"));
      try {
        Thread.sleep(1000);
      } catch (Exception e) {
        e.printStackTrace();
      }
      ((JSONNIOTransport<Integer>) niots[0]).sendToIDActual(1,
              JSONify(msgNum++, "Third hello back from 0 to 1"));
      ((JSONNIOTransport<Integer>) niots[1]).sendToIDActual(
              0,
              JSONify(msgNum++,
                      "Thank you for all the hellos back from 1 to 0"));
      /**
       * **********************************************************************
       */

      int seqTestNum = 1;
      Thread.sleep(2000);
      System.out.println("\n\n\nBeginning test of " + seqTestNum
              + " random, sequential messages");
      Thread.sleep(1000);

      /**
       * **********************************************************************
       */
      // Create the remaining nodes up to nNodes
      for (int i = smallNNodes; i < nNodes; i++) {
        niots[i] = new JSONNIOTransport<Integer>(i, snc, workers[i]);
        new Thread(niots[i]).start();
      }

      // Test a random, sequential communication pattern
      for (int i = 0; i < nNodes * seqTestNum; i++) {
        int k = (int) (Math.random() * nNodes);
        int j = (int) (Math.random() * nNodes);
        System.out.println("Message " + i + " with msgNum " + msgNum);
        ((JSONNIOTransport<Integer>) niots[k]).sendToIDActual(j,
                JSONify(msgNum++, "Hello from " + k + " to " + j));
      }

      int oneToOneTestNum = 1;
      /**
       * **********************************************************************
       */
      Thread.sleep(1000);
      System.out.println("\n\n\nBeginning test of " + oneToOneTestNum
              * nNodes
              + " random, concurrent, 1-to-1 messages with emulated delays");
      Thread.sleep(1000);
      /**
       * **********************************************************************
       */
      // Test a random, concurrent communication pattern with emulated delays
      ScheduledExecutorService execpool
              = Executors.newScheduledThreadPool(5);
      class TX extends TimerTask {

        JSONNIOTransport<Integer> sndr = null;
        private int rcvr = -1;
        int msgNum = -1;

        TX(int i, int id, JSONNIOTransport<?>[] n, int m) {
          sndr = (JSONNIOTransport<Integer>) n[i];
          rcvr = id;
          msgNum = m;
        }

        TX(JSONNIOTransport<Integer> niot, int id, int m) {
          sndr = niot;
          rcvr = id;
          msgNum = m;
        }

        public void run() {
          try {
            sndr.sendToIDActual(
                    rcvr,
                    JSONify(msgNum, "Hello from " + sndr.myID
                            + " to " + rcvr));
          } catch (IOException e) {
            e.printStackTrace();
          } catch (JSONException e) {
            e.printStackTrace();
          }
        }
      }
      JSONDelayEmulator.emulateDelays();

      JSONNIOTransport<Integer> concurrentSender
              = new JSONNIOTransport<Integer>(nNodes, snc, workers[nNodes]);
      new Thread(concurrentSender).start();
      ScheduledFuture<?>[] futuresRandom
              = new ScheduledFuture[nNodes * oneToOneTestNum];
      for (int i = 0; i < nNodes * oneToOneTestNum; i++) {
        TX task = new TX(concurrentSender, 0, msgNum++);
        System.out.println("Scheduling random message " + i
                + " with msgNum " + msgNum);
        futuresRandom[i]
                = execpool.schedule(task, 0, TimeUnit.MILLISECONDS);
      }
      for (int i = 0; i < nNodes * oneToOneTestNum; i++) {
        try {
          futuresRandom[i].get();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }

      /**
       * **********************************************************************
       */
      Thread.sleep(1000);
      System.out.println("\n\n\nBeginning test of random, concurrent, "
              + " any-to-any messages with emulated delays");
      Thread.sleep(1000);
      /**
       * **********************************************************************
       */

      int load = nNodes * 25;
      int msgsToFailed = 0;
      ScheduledFuture<?>[] futures = new ScheduledFuture[load];
      for (int i = 0; i < load; i++) {
        int k = (int) (Math.random() * nNodes);
        int j = (int) (Math.random() * nNodes);
        // long millis = (long)(Math.random()*1000);

        if (i % 100 == 0) {
          j = nNodes + 1; // Periodically try sending to a non-existent node
          msgsToFailed++;
        }

        TX task = new TX(k, j, niots, msgNum++);
        System.out.println("Scheduling random message " + i
                + " with msgNum " + msgNum);
        futures[i]
                = (ScheduledFuture<?>) execpool.schedule(task, 0,
                        TimeUnit.MILLISECONDS);
      }
      int numExceptions = 0;
      for (int i = 0; i < load; i++) {
        try {
          futures[i].get();
        } catch (Exception e) {
          // e.printStackTrace();
          numExceptions++;
        }
      }

      /**
       * **********************************************************************
       */
      Thread.sleep(2000);
      System.out.println("\n\n\nPrinting overall stats. Number of exceptions =  "
              + numExceptions);
      System.out.println((new NIOInstrumenter() + "\n"));
      boolean pending = false;
      for (int i = 0; i < nNodes; i++) {
        if (niots[i].getPendingSize() > 0) {
          System.out.println("Pending messages at node " + i + " : "
                  + niots[i].getPendingSize());
          pending = true;
        }
      }
      int missing = NIOInstrumenter.getMissing();
      assert (pending == false || missing == msgsToFailed) : "Unsent pending messages in NIO";
      for (NIOTransport<?> niot : niots) {
        niot.stop();
      }
      concurrentSender.stop();
      execpool.shutdown();

      if (!pending || missing == msgsToFailed) {
        System.out.println("\nSUCCESS: no pending messages to non-failed nodes!");
      }

    } catch (IOException e) {
      e.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
