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
package edu.umass.cs.gnsclient.examples;

import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;

/**
 * Creates some DNS records for testing the GNS DNS service.
 *
 * @author westy
 */
public class CreateDnsRecord {
  private static final String ACCOUNT_ALIAS = "admin@gns.name"; // REPLACE THIS WITH YOUR ACCOUNT ALIAS
  private static GNSClientCommands client;
  private static GuidEntry accountGuid;

  /**
   *
   * @param args
   * @throws IOException
   * @throws InvalidKeySpecException
   * @throws NoSuchAlgorithmException
   * @throws ClientException
   * @throws InvalidKeyException
   * @throws SignatureException
   * @throws Exception
   */
  public static void main(String[] args) throws IOException,
          InvalidKeySpecException, NoSuchAlgorithmException, ClientException,
          InvalidKeyException, SignatureException, Exception {

    if (args.length != 2) { 
      System.out.println("Usage: edu.umass.cs.gnsclient.examples.CreateDnsRecord <host> <address>");
      System.exit(0);
    }
    client = new GNSClientCommands(new GNSClient());
    try {
      accountGuid = GuidUtils.lookupOrCreateAccountGuid(client, ACCOUNT_ALIAS, "password", true);
    } catch (Exception e) {
      System.out.println("Exception during accountGuid creation: " + e);
      System.exit(1);
    }
    System.out.println("Client connected to GNS");
    
    createAnARecord(args[0], args[1]);
    
    client.close();
    
    System.exit(0);
  }
  
  // Creates an A record in the GNS
  private static void createAnARecord(String domain, String address) throws Exception {
    // domains need to end with a period
    if (!domain.endsWith(".")) {
      domain += ".";
    }
    GuidEntry guid = GuidUtils.lookupOrCreateGuid(client, accountGuid, domain);
    client.fieldUpdate(guid, "A", address);
    System.out.println("Value of A in " + guid.getEntityName() + " is " + client.fieldRead(guid, "A"));
    
  }

}
