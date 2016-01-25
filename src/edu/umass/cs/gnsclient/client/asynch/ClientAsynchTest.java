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
 *
 * @author westy
 */
public class ClientAsynchTest {

  private static Request receivedResponse;
  private static long readTimeout = 10000;

  public static void main(String args[]) {
    ClientAsynchBase client;
    try {
      client = new ClientAsynchBase();
    } catch (IOException e) {
      GNSClient.getLogger().severe("Problem creating client:" + e);
      return;
    }
    try {
      Object monitor = new Object();
      receivedResponse = null;
      GuidEntry guidEntry = client.newAccountGuidCreate("test@gns.name", "frank",
              new RequestCallback() {
                @Override
                public void handleResponse(Request response) {
                  synchronized (monitor) {
                    monitor.notifyAll();
                  }
                  receivedResponse = response;
                }
              });
      synchronized (monitor) {
        long monitorStartTime = System.currentTimeMillis();
        while (receivedResponse == null && (readTimeout == 0 || System.currentTimeMillis() - monitorStartTime < readTimeout)) {
          monitor.wait(readTimeout);
        }
        if (readTimeout != 0 && System.currentTimeMillis() - monitorStartTime >= readTimeout) {
          GNSClient.getLogger().info("Timeout!");
        }
      }
      System.out.println("##### Guid: " + guidEntry.getGuid());
      System.out.println("##### Received response: " + receivedResponse);
    } catch (Exception e) {
      GNSClient.getLogger().severe("Problem executing command:" + e);
    }
    
    try {
      Object monitor = new Object();
      receivedResponse = null;
      long id = client.lookupGuid("test@gns.name", 
              new RequestCallback() {
                @Override
                public void handleResponse(Request response) {
                  synchronized (monitor) {
                    monitor.notifyAll();
                  }
                  receivedResponse = response;
                }
              });
      synchronized (monitor) {
        long monitorStartTime = System.currentTimeMillis();
        while (receivedResponse == null && (readTimeout == 0 || System.currentTimeMillis() - monitorStartTime < readTimeout)) {
          monitor.wait(readTimeout);
        }
        if (readTimeout != 0 && System.currentTimeMillis() - monitorStartTime >= readTimeout) {
          GNSClient.getLogger().info("Timeout!");
        }
      }
      System.out.println("##### Received response: " + receivedResponse);
    } catch (Exception e) {
      GNSClient.getLogger().severe("Problem executing command:" + e);
    }
    
  }

}
