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
 *  Initial developer(s): Westy, Emmanuel Cecchet
 *
 */
package edu.umass.cs.gnsclient.examples;

import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;

/**
 * In this example we create an account to write and read back some information in the GNS.
 *  <p>
 This client uses the GNSClientCommandsExtended class which contains more weird
 client methods than BasicUniversalTcpClient.
 
 Note: This example cheats during account guid creation in that it creates the account 
 guid and then uses the known secret to verify the account instead of making the user 
 verify the account manually or deal with the private key.
 *
 * @author westy
 */
public class SimpleClientExample {

  private static final String ACCOUNT_ALIAS = "admin@gns.name"; // REPLACE THIS WITH YOUR ACCOUNT ALIAS
  private static final String PASSWORD = "password";
  private static GNSClientCommands client;
  private static GuidEntry accountGuid;

  public static void main(String[] args) throws IOException,
          InvalidKeySpecException, NoSuchAlgorithmException, ClientException,
          InvalidKeyException, SignatureException, Exception {
    
    // Create the client. Connects to a default reconfigurator as specified in gigapaxos.properties file.
    client = new GNSClientCommands();
    try {
      // Create an account guid if one doesn't already exists.
      // The true makes it verbosely print out what it is doing.
      // The password is for future use.
      // Note that lookupOrCreateAccountGuid "cheats" by bypassing the account verification
      // mechanisms.
      accountGuid = GuidUtils.lookupOrCreateAccountGuid(client, ACCOUNT_ALIAS, PASSWORD, true);
    } catch (Exception e) {
      System.out.println("Exception during accountGuid creation: " + e);
      System.exit(1);
    }
    System.out.println("Client connected to GNS");

    // Retrive the GUID using the account id
    String guid = client.lookupGuid(ACCOUNT_ALIAS);
    System.out.println("Retrieved GUID for " + ACCOUNT_ALIAS + ": " + guid);

    // Get the public key from the GNS
    PublicKey publicKey = client.publicKeyLookupFromGuid(guid);
    System.out.println("Retrieved public key: " + publicKey.toString());

    // Use the GuidEntry create an new record in the GNS
    client.fieldUpdate(accountGuid, "homestate", "Florida");
    System.out.println("Added homestate -> Florida record to the GNS for GUID " + accountGuid.getGuid());

    // Retrive that record from the GNS
    String result = client.fieldRead(accountGuid.getGuid(), "homestate", accountGuid);
    System.out.println("Result of read location: " + result);
    
    System.exit(0);
  }
  
}
