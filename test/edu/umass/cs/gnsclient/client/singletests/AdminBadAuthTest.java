/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *
 */
package edu.umass.cs.gnsclient.client.singletests;

import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GNSClientCommands;

import edu.umass.cs.gnsserver.utils.DefaultGNSTest;
import java.io.IOException;
import java.net.InetSocketAddress;


import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * Expects ClientExceptions for each admin command since this test is intended to be used by a client that does not have the correct MUTUAL_AUTH keys.
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class AdminBadAuthTest extends DefaultGNSTest {

  /**
   *
   * @author Brendan Teich
   *
   * This class overrides GNSClient and its AsynchClient in order to force it
   * to send MUTUAL_AUTH requests to the SERVER_AUTH client port.
   *
   */
  private static class BadClient extends GNSClientCommands {

    /**
     * @throws IOException
     */
    public BadClient() throws IOException {
      super((InetSocketAddress) null);
    }
  }

  private static GNSClientCommands clientCommands;
  private static BadClient badClient;


  
  /**
   *
   * @throws IOException
   */
  @BeforeClass
  public static void setupBeforeClass() throws IOException {
    System.out.println("Starting client");

    clientCommands = new GNSClientCommands();

    // Make all the reads be coordinated
    clientCommands.setForceCoordinatedReads(true);
    // arun: connectivity check embedded in GNSClient constructor
    boolean connected = clientCommands instanceof GNSClient;
    if (connected) {
      System.out.println("Client created and connected to server.");
    }
 
    badClient = new BadClient();
    badClient.setForceCoordinatedReads(true);
    connected = badClient instanceof GNSClient;
    if (connected) {
      System.out.println("BadClient created and connected to server.");
    }
  }

  /**
   *
   * @throws Exception
   */
  @Test
  public void test_04_Dump() throws Exception {
    clientCommands.dump();
  }

//  /**
//   *
//   * @throws Exception
//   */
//  @Test(expected = ClientException.class)
//  public void test_14_Dump_ClientPort() throws Exception {
//    badClient.dump();
//  }

}
