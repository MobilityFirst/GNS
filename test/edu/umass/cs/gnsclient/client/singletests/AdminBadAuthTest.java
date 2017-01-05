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
 *  Initial developer(s): Westy
 *
 */
package edu.umass.cs.gnsclient.client.singletests;


import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.GNSClient.AsyncClient;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.utils.ThreadUtils;
import edu.umass.cs.gnsserver.gnsapp.packet.Packet;
import edu.umass.cs.nio.SSLDataProcessingWorker.SSL_MODES;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Set;

import static org.junit.Assert.*;

import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * Expects ClientExceptions for each admin command since this test is intended to be used by a client that does not have the correct MUTUAL_AUTH keys.
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class AdminBadAuthTest {
	/**
	 * 
	 * @author Brendan Teich
	 * 
	 * This class overrides GNSClient and its AsynchClient in order to force it
	 *  to send MUTUAL_AUTH requests to the SERVER_AUTH client port.
	 *
	 */
	private static class BadClient extends GNSClientCommands{
		  private BadClient(Set<InetSocketAddress> reconfigurators)
		          throws IOException {
		    this.asyncClient = new AsyncClientBad(reconfigurators,
		            ReconfigurationConfig.getClientSSLMode(),
		            ReconfigurationConfig.getClientPortOffset(), true);
		  }
		  /**
		   * @throws IOException
		   */
		  public BadClient() throws IOException {
		    super((InetSocketAddress) null);
		  }
		/**
		   * Straightforward async client implementation that expects only one packet
		   * type, {@link Packet.PacketType.COMMAND_RETURN_VALUE}.
		   */
		  class AsyncClientBad extends AsyncClient{

		    public AsyncClientBad(Set<InetSocketAddress> reconfigurators,
		    		SSL_MODES sslMode, int clientPortOffset,
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

  private static GNSClientCommands client;
  private static BadClient badClient;
  private static GuidEntry masterGuid;
	private static final String DEFAULT_ACCOUNT_ALIAS = "support@gns.name";

	private static String accountAlias = DEFAULT_ACCOUNT_ALIAS; 
																
	private static final String PASSWORD = "password";

  /*public AdminTest() {
    if (client == null) {
     try {
        client = new GNSClientCommands();
        client.setForceCoordinatedReads(true);
      } catch (IOException e) {
        fail("Exception creating client: " + e);
      }
    }
  }*/

  /**
   *
   * @throws IOException
   */

  @BeforeClass
  public static void setupBeforeClass() throws IOException{
	  System.out.println("Starting client");

		client = new GNSClientCommands();
		
		// Make all the reads be coordinated
		client.setForceCoordinatedReads(true);
		// arun: connectivity check embedded in GNSClient constructor
		boolean connected = client instanceof GNSClient;
		if (connected) {
			System.out.println("Client created and connected to server.");
		}
		//
		int tries = 5;
		boolean accountCreated = false;

		
		do {
			try {
				System.out.println("Creating account guid: " + (tries - 1)
						+ " attempt remaining.");
				masterGuid = GuidUtils.lookupOrCreateAccountGuid(client,
						accountAlias, PASSWORD, true);
				accountCreated = true;
			} catch (Exception e) {
				e.printStackTrace();
				ThreadUtils.sleep((5 - tries) * 5000);
			}
		} while (!accountCreated && --tries > 0);
		if (accountCreated == false) {
			fail("Failure setting up account guid; aborting all tests.");
		}
		
		badClient = new BadClient();
		badClient.setForceCoordinatedReads(true);
		connected = badClient instanceof GNSClient;
		if (connected) {
			System.out.println("BadClient created and connected to server.");
		}
		/*
		//
		tries = 5;
		accountCreated = false;

		
		do {
			try {
				System.out.println("Creating account guid: " + (tries - 1)
						+ " attempt remaining.");
				masterGuid = GuidUtils.lookupOrCreateAccountGuid(client,
						accountAlias, PASSWORD, true);
				accountCreated = true;
			} catch (Exception e) {
				e.printStackTrace();
				ThreadUtils.sleep((5 - tries) * 5000);
			}
		} while (!accountCreated && --tries > 0);
		if (accountCreated == false) {
			fail("Failure setting up account guid; aborting all tests.");
		}*/
  }
  
  /*These tests check if sending an admin command on the MUTUAL_AUTH 
   * port with the wrong keystore or without a keystore fails like it should. */

  /**
   *
   * @throws Exception
   */

//  @Test(expected=ClientException.class)
//  public void test_01_ParameterGet() throws Exception {
//      String result = client.parameterGet("email_verification");
//  }
//
//  /**
//   *
//   * @throws Exception
//   */
//  @Test(expected=ClientException.class)
//  public void test_02_ParameterSet() throws Exception {
//      client.parameterSet("max_guids", 2000);
//      String result = client.parameterGet("max_guids");
//  }
//  
//  /**
//   *
//   * @throws Exception
//   */
//  @Test(expected=ClientException.class)
//  public void test_03_ParameterList() throws Exception {
//      String result = client.parameterList();
//  }
  
  /**
   *
   * @throws Exception
   */
  @Test(expected=ClientException.class)
  public void test_04_Dump() throws Exception {
      String result = client.dump();
  }
  
  /**
//   *
//   * @throws Exception
//   */
//  @Test(expected=ClientException.class)
//  public void test_05_ClearCache() throws Exception {
//      String result = client.clearCache();
//  }
//  
//  /**
//   *
//   * @throws Exception
//   */
//  @Test(expected=ClientException.class)
//  public void test_06_DumpCache() throws Exception {
//      String result = client.dumpCache();
//  }
//  
  
  /*These tests check if sending an admin command on the client port
   * without mutual auth fails like it should. */

  /**
   *
   * @throws Exception
   */

  
//  @Test(expected=ClientException.class)
//  public void test_11_ParameterGet_ClientPort() throws Exception {
//      String result =  badClient.parameterGet("email_verification");
//  }
//
//  /**
//   *
//   * @throws Exception
//   */
//  @Test(expected=ClientException.class)
//  public void test_12_ParameterSet_ClientPort() throws Exception {
//      badClient.parameterSet("max_guids", 2000);
//      String result = badClient.parameterGet("max_guids");
//  }
//  
//  /**
//   *
//   * @throws Exception
//   */
//  @Test(expected=ClientException.class)
//  public void test_13_ParameterList_ClientPort() throws Exception {
//      String result =  badClient.parameterList();
//  }
  
  /**
   *
   * @throws Exception
   */
  @Test(expected=ClientException.class)
  public void test_14_Dump_ClientPort() throws Exception {
      String result =  badClient.dump();
  }
  
//  /**
//   *
//   * @throws Exception
//   */
//  @Test(expected=ClientException.class)
//  public void test_15_ClearCache_ClientPort() throws Exception {
//      String result =  badClient.clearCache();
//  }
//  
//  /**
//   *
//   * @throws Exception
//   */
//  @Test(expected=ClientException.class)
//  public void test_16_DumpCache_ClientPort() throws Exception {
//      String result =  badClient.dumpCache();
//  }

}
