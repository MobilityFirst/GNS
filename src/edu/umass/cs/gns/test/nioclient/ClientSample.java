package edu.umass.cs.gns.test.nioclient;

import edu.umass.cs.gns.clientsupport.UpdateOperation;
import edu.umass.cs.gns.database.ColumnFieldType;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.gns.nsdesign.nodeconfig.GNSNodeConfig;
import edu.umass.cs.gns.nsdesign.packet.*;
import edu.umass.cs.gns.util.ResultValue;
import edu.umass.cs.gns.util.Util;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.net.InetAddress;

/**
 * Sample code written using DBClient that demonstrates read/write/add/remove operations for a name. It shows which
 * how to construct JSON objects to send a request, and how to parse the JSON objects received as responses.
 *
 * The DBClient provides an asynchronous API, therefore this class uses a wait/notify mechanism to match requests with
 * responses.
 * Created by abhigyan on 6/21/14.
 */
public class ClientSample extends AbstractPacketDemultiplexer {

  private final InetAddress lnsAddress;
  private final int lnsPort;
  private final int clientPort;

  private final Object monitor = new Object();

  private JSONObject mostRecentResponse;

  private boolean noAssert = false;

  public ClientSample(InetAddress lnsAddress, int lnsPort, int clientPort) {

    this.lnsAddress = lnsAddress;
    this.lnsPort = lnsPort;
    this.clientPort = clientPort;
  }

  public ClientSample(InetAddress lnsAddress, int lnsPort, int clientPort, boolean noAssert) {
    this.lnsAddress = lnsAddress;
    this.lnsPort = lnsPort;
    this.clientPort = clientPort;
    this.noAssert = noAssert;
  }

  void startClient() throws JSONException, IOException, InterruptedException {
    try {
      DBClient dbClient = new DBClient(lnsAddress, lnsPort, clientPort, this);
      GNS.getLogger().info("Client starting to send requests ....");
      final int repeatCycles = 10000; // number of times sequence of ADD, SINGLE_FIELD_REMOVE, etc is repeated.
      final int maxTries = 5; // number of times a lookup is retried.
      int reqCount = 0; // counter to assign request IDs
      int retriedLookups = 0;
      int retriedUpdates = 0;
      for (int i = 0; i < repeatCycles; i++) {
        // send add request
        String name = "testName";
        String key = "testKey" + i;
        String firstValue = "firstValue-"+ Util.randomString(10) ;
        ResultValue rv = new ResultValue();
        rv.add(firstValue);
        AddRecordPacket addRecordPacket = new AddRecordPacket(AddRecordPacket.LOCAL_SOURCE_ID, ++reqCount, name, key, rv,
                null, 0);
        dbClient.sendRequest(addRecordPacket.toJSONObject());
        waitForResponse();
        ConfirmUpdatePacket confirmPkt = new ConfirmUpdatePacket(mostRecentResponse);
        if (!noAssert) {
          assert confirmPkt.getRequestID() == reqCount : "Add operation failed: Request ID mismatch." + confirmPkt;
          assert confirmPkt.isSuccess() : "Add operation failed: Failure return code. " + confirmPkt;
        }
        GNS.getLogger().info("SUCCESS: Name added to GNS");

        // send lookup for name

        int tryCount = 0;
        while (tryCount < maxTries) {
          try {
            DNSPacket dnsPacket = new DNSPacket(DNSPacket.LOCAL_SOURCE_ID, ++reqCount, name, key, null, 
                   ColumnFieldType.LIST_STRING, null, null, null);
            dbClient.sendRequest(dnsPacket.toJSONObject());
            waitForResponse();
            DNSPacket dnsResponse = new DNSPacket(mostRecentResponse);
            if (!noAssert) {
              assert dnsResponse.getQueryId() == reqCount : "Lookup failure: Request ID mismatch. " + dnsResponse;
              assert !dnsResponse.containsAnyError() : "Lookup failure: Error. " + dnsResponse;
              assert dnsResponse.getRecordValue().getAsArray(key).get(0).equals(firstValue) : "Lookup failure: Value mismatch. Initial value not found. " + dnsResponse;
            }
          } catch (AssertionError e) {
            GNS.getLogger().info("Retrying lookup to read initial value ... ");
            Thread.sleep(1000);
            tryCount ++;
            continue;
          }
          break;
        }
        if (tryCount > 0) retriedLookups++;
        assert tryCount < maxTries: "Lookup failure: Initial value not found after retries";

        GNS.getLogger().info("SUCCESS: Name lookup returned initial value");

        // send update
        String secondValue = "secondValue" + Util.randomString(10);
        rv = new ResultValue();
        rv.add(secondValue);
        UpdatePacket updatePacket = new UpdatePacket(UpdatePacket.LOCAL_SOURCE_ID, ++reqCount, name, key, rv, null, 0, UpdateOperation.SINGLE_FIELD_REPLACE_ALL,
                null, 0, null, null, null);
        dbClient.sendRequest(updatePacket.toJSONObject());
        waitForResponse();
        confirmPkt = new ConfirmUpdatePacket(mostRecentResponse);
        if (!noAssert) {
          assert confirmPkt.getRequestID() == reqCount : "Update operation failed: Request ID mismatch " + confirmPkt;
          assert confirmPkt.isSuccess() : "Update operation failed: Error " + confirmPkt;
        }
        GNS.getLogger().info("SUCCESS: Name updated in GNS: " + confirmPkt);

        // read and test if updated value is received

        tryCount = 0;
        String valueRead = null;
        while (tryCount < maxTries) {
          try {
            DNSPacket dnsPacket = new DNSPacket(DNSPacket.LOCAL_SOURCE_ID, ++reqCount, name, key, null, 
                    ColumnFieldType.LIST_STRING,
                    null, null, null);
            dbClient.sendRequest(dnsPacket.toJSONObject());
            waitForResponse();
            DNSPacket dnsResponse = new DNSPacket(mostRecentResponse);
            if (!noAssert) {
              assert dnsResponse.getQueryId() == reqCount : "Lookup failure: Request ID mismatch. " + dnsResponse;
              assert !dnsResponse.containsAnyError() : "Lookup failure: Error. " + dnsResponse;
              assert dnsResponse.getRecordValue().getAsArray(key).get(0).equals(secondValue) : "Lookup failure: Value mismatch. Updated value not found. " + dnsResponse;
              valueRead = (String) dnsResponse.getRecordValue().getAsArray(key).get(0);
            }
          } catch (AssertionError e) {
            GNS.getLogger().info("Retrying lookup to read updated value ... " + "First: " + firstValue + " Updated: " + secondValue + " Read: " + valueRead);
            Thread.sleep(1000);
            tryCount ++;
            continue;
          }
          break;
        }
        if (tryCount > 0) retriedLookups++;
        assert tryCount < maxTries: "Lookup failure: Updated value not found after retries";
        GNS.getLogger().info("SUCCESS: Name lookup returned updated value");

        // remove name
        RemoveRecordPacket removePacket = new RemoveRecordPacket(RemoveRecordPacket.INTERCESSOR_SOURCE_ID, ++reqCount, name, null);
        dbClient.sendRequest(removePacket.toJSONObject());
        waitForResponse();
        confirmPkt = new ConfirmUpdatePacket(mostRecentResponse);
        if (!noAssert) {
          assert confirmPkt.getRequestID() == reqCount: "Remove operation failed: Request ID mismatch. " + confirmPkt;
          assert confirmPkt.isSuccess(): "Remove operation failed. Error. " + confirmPkt;
        }
        GNS.getLogger().info("SUCCESS: Name removed from GNS");

        GNS.getLogger().info("Client received all responses ... SUCCESS. Iteration: " + i);
        Thread.sleep(1000);
      }
      GNS.getLogger().info("Number of lookups retried: " + retriedLookups);
      GNS.getLogger().info("Test Complete ... SUCCESS.");
    } catch (AssertionError e) {
      e.printStackTrace();
    }
    System.exit(2);
  }

  private void waitForResponse() throws InterruptedException {
    synchronized (monitor) {
      monitor.wait();
    }
  }

  @Override
  public boolean handleJSONObject(JSONObject json) {
    GNS.getLogger().fine("Client recvd response ... ");
    mostRecentResponse = json;
    synchronized (monitor) {
      monitor.notify();
    }
    return true;
  }

  // commented out when the LNSs lost their ids
//  public static void main(String[] args) throws IOException, JSONException, InterruptedException {
//    GNS.consoleOutputLevel = GNS.statConsoleOutputLevel = GNS.fileLoggingLevel = GNS.statFileLoggingLevel = "INFO";
//
//    // A client only needs to know IP/port of a local name server.
//    String nodeConfigFile = args[0];
//    // port on which client is listening for responses from a local name server.
//    int myPort = Integer.parseInt(args[1]);
//    GNSNodeConfig gnsNodeConfig = new GNSNodeConfig(nodeConfigFile, 0);
//    int lnsID = -1;
//    for (int nodeID: gnsNodeConfig.getLocalNameServerIDs()) {
//      lnsID = nodeID;
//      break;
//    }
//
//    if (lnsID != -1) {
//      ClientSample cs = new ClientSample(gnsNodeConfig.getNodeAddress(lnsID), gnsNodeConfig.getLnsDbClientPort(lnsID),
//              myPort);
//      cs.startClient();
//    } else {
//      GNS.getLogger().fine(" No local name servers found in config file.");
//    }
//  }
}
