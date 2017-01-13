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
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.utils.ThreadUtils;

import java.io.IOException;
import java.net.InetSocketAddress;

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
		  /**
		   * @throws IOException
		   */
		  public BadClient() throws IOException {
		    super((InetSocketAddress) null);
		  }
	}

  private static GNSClientCommands client;
  private static BadClient badClient;
	private static final String DEFAULT_ACCOUNT_ALIAS = "support@gns.name";

	private static String accountAlias = DEFAULT_ACCOUNT_ALIAS; 
																
	private static final String PASSWORD = "password";

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
				GuidUtils.lookupOrCreateAccountGuid(client,
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
  }
  
  /**
   *
   * @throws Exception
   */
  @Test(expected=ClientException.class)
  public void test_04_Dump() throws Exception {
      client.dump();
  }
  
  /**
   *
   * @throws Exception
   */
  @Test(expected=ClientException.class)
  public void test_14_Dump_ClientPort() throws Exception {
      badClient.dump();
  }
  
}

