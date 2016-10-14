/*
 * Copyright (C) 2016
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gnscommon.asynch;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestCallback;
import edu.umass.cs.gnsclient.client.GNSClientConfig;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;

import edu.umass.cs.gnsserver.localnameserver.LNSPacketDemultiplexer;
import edu.umass.cs.gnsserver.main.GNSConfig;
import java.io.IOException;
import java.util.logging.Level;

import org.json.JSONException;

/**
 * This tests the asynch client. It uses wait / notify so we
 * can do one step followed by another step. This usage isn't part of the
 * asynch client, it's just a way to do this test.
 *
 * @author westy
 */
public class ClientAsynchTest {

  private static Request receivedResponse;
  private static final long READ_TIMEOUT = 10000;
  private static final Object MONITOR = new Object();
  // make a call back that notifys any waits and records the response

  private static final RequestCallback CALLBACK = new RequestCallback() {

    @Override
    public void handleResponse(Request response) {
      synchronized (MONITOR) {
        MONITOR.notifyAll();
      }
      receivedResponse = response;
    }
  };

  /**
   *
   * @param args
   */
  public static void main(String args[]) {
    ClientAsynchBase client;
    try {
      client = new ClientAsynchBase();
    } catch (IOException e) {
      GNSClientConfig.getLogger().log(Level.SEVERE, "Problem creating client:{0}", e);
      return;
    }
    GuidEntry guidEntry = null;
    try {
      receivedResponse = null;
      guidEntry = client.accountGuidCreate("test@gns.name", "frank", CALLBACK);
      System.out.println("##### Guid: " + guidEntry.getGuid());
      waitForResponse();
      System.out.println("##### Received response: " + receivedResponse);
    } catch (Exception e) {
      GNSClientConfig.getLogger().log(Level.SEVERE, "Problem executing command:{0}", e);
    }
    if (guidEntry == null) {
      GNSClientConfig.getLogger().info("Guid entry is null!");
      return;
    }
    // Try to lookup the account info
    try {
      receivedResponse = null;
      long id = client.lookupAccountRecord(guidEntry.getGuid(), CALLBACK);
      System.out.println("##### id: " + id);
      waitForResponse();
      System.out.println("##### Received response: " + receivedResponse);
    } catch (IOException | ClientException | JSONException e) {
      GNSClientConfig.getLogger().log(Level.SEVERE, "Problem executing command:{0}", e);
      System.exit(-1);
    }
    System.exit(0);
  }

  private static void waitForResponse() {
    try {
      synchronized (MONITOR) {
        long monitorStartTime = System.currentTimeMillis();
        while (receivedResponse == null && (READ_TIMEOUT == 0 || System.currentTimeMillis() - monitorStartTime < READ_TIMEOUT)) {
          MONITOR.wait(READ_TIMEOUT);
        }
        if (READ_TIMEOUT != 0 && System.currentTimeMillis() - monitorStartTime >= READ_TIMEOUT) {
          GNSClientConfig.getLogger().info("Timeout!");
        }
      }
    } catch (InterruptedException x) {
      GNSClientConfig.getLogger().log(Level.SEVERE, "Wait for return packet was interrupted {0}", x);
    }
  }

}
