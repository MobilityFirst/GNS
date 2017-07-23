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
package edu.umass.cs.gnsclient.client.singletests.failingtests;

import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GNSClientCommands;

import edu.umass.cs.gnsserver.utils.DefaultGNSTest;
import edu.umass.cs.nio.SSLDataProcessingWorker;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import java.io.IOException;
import java.net.InetSocketAddress;

import java.util.Set;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * Expects ClientExceptions for each admin command since this test is intended 
 * to be used by a client that does not have the correct MUTUAL_AUTH keys.
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
  private static class BadClient extends GNSClient {

    private BadClient(Set<InetSocketAddress> reconfigurators)
            throws IOException {
      this.asyncClient = new AsyncClientBad(reconfigurators,
              ReconfigurationConfig.getClientSSLMode(),
              ReconfigurationConfig.getClientPortOffset(), true);
    }

    /**
     * Straightforward async client implementation that expects only one packet
     * type, {@link Packet.PacketType.COMMAND_RETURN_VALUE}.
     */
    class AsyncClientBad extends AsyncClient {

      public AsyncClientBad(Set<InetSocketAddress> reconfigurators,
              SSLDataProcessingWorker.SSL_MODES sslMode, int clientPortOffset,
              boolean checkConnectivity) throws IOException {
        super(reconfigurators, sslMode, clientPortOffset,
                checkConnectivity);
      }

      /**
       * This returns null since this client always sends commands to the client port.
       */
      @SuppressWarnings("javadoc")
      @Override
      public Set<IntegerPacketType> getMutualAuthRequestTypes() {
        return null;
      }

    }
  }

  private static GNSClientCommands clientCommands;
  private static GNSClientCommands badClient;

  /**
   *
   * @throws IOException
   */
  @BeforeClass
  public static void setupBeforeClass() throws IOException {
    System.out.println("Starting client");

    clientCommands = new GNSClientCommands(client);

    // Make all the reads be coordinated
    clientCommands.setForceCoordinatedReads(true);
    // arun: connectivity check embedded in GNSClient constructor
    
    System.out.println("Client created and connected to server.");
    

    badClient = new GNSClientCommands
    		(new BadClient(ReconfigurationConfig.getReconfiguratorAddresses()));
    badClient.setForceCoordinatedReads(true);
      System.out.println("BadClient created and connected to server.");
  }

  /**
   *
   * @throws Exception
   */
  @Test
  public void test_04_Dump() throws Exception {
    System.out.println(clientCommands.dump());
  }

  /**
   *
   */
  @Test
  public void test_14_Dump_BadClient() {
    try {
      System.out.println(badClient.dump());
      Assert.fail("Should throw an exception.");
    } catch (Exception e) {
      System.out.println("Dump_BadClient throws an exception (expected):" + e.getMessage());
    }
  }

}
