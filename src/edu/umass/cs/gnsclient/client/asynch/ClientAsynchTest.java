/*
 * Copyright (C) 2016
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gnsclient.client.asynch;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestCallback;
import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GuidEntry;
import java.io.IOException;

/**
 * This tests the asynch client. It uses wait / notify so we
 * can do one step followed by another step. This usage isn't part of the
 * asynch client, it's just a way to do this test.
 *
 * @author westy
 */
public class ClientAsynchTest {

  private static Request receivedResponse;
  private static long readTimeout = 10000;
  private static final Object monitor = new Object();
  // make a call back that notifys any waits and records the response
  private static RequestCallback callback = (Request response) -> {
    synchronized (monitor) {
      monitor.notifyAll();
    }
    receivedResponse = response;
  };

  public static void main(String args[]) {
    ClientAsynchBase client;
    try {
      client = new ClientAsynchBase();
    } catch (IOException e) {
      GNSClient.getLogger().severe("Problem creating client:" + e);
      return;
    }
    GuidEntry guidEntry = null;
    try {
      receivedResponse = null;
      guidEntry = client.accountGuidCreate("test@gns.name", "frank", callback);
      System.out.println("##### Guid: " + guidEntry.getGuid());
      waitForResponse();
      System.out.println("##### Received response: " + receivedResponse);
    } catch (Exception e) {
      GNSClient.getLogger().severe("Problem executing command:" + e);
    }
    if (guidEntry == null) {
      GNSClient.getLogger().info("Guid entry is null!");
      return;
    }
    // Try to lookup the account info
    try {
      receivedResponse = null;
      long id = client.lookupAccountRecord(guidEntry.getGuid(), callback);
      System.out.println("##### id: " + id);
      waitForResponse();
      System.out.println("##### Received response: " + receivedResponse);
    } catch (Exception e) {
      GNSClient.getLogger().severe("Problem executing command:" + e);
      System.exit(-1);
    }
    System.exit(0);
  }

  private static void waitForResponse() {
    try {
      synchronized (monitor) {
        long monitorStartTime = System.currentTimeMillis();
        while (receivedResponse == null && (readTimeout == 0 || System.currentTimeMillis() - monitorStartTime < readTimeout)) {
          monitor.wait(readTimeout);
        }
        if (readTimeout != 0 && System.currentTimeMillis() - monitorStartTime >= readTimeout) {
          GNSClient.getLogger().info("Timeout!");
        }
      }
    } catch (InterruptedException x) {
      GNSClient.getLogger().severe("Wait for return packet was interrupted " + x);
    }
  }

}
