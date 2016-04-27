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
import edu.umass.cs.gnsclient.client.util.ServerSelectDialog;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.net.InetSocketAddress;

/**
 * In this example we create an account to write and read back some information in the GNS.
 *  <p>
 This client uses the GNSClientCommandsExtended class which contains more weird
 client methods than BasicUniversalTcpClient.
 
 Note: This example cheats during account guid creation in that it creates the account 
 guid and then uses the known secret to verify the account instead of making the user 
 verify the account manually deal with the private key.
 *
 * @author westy
 */
public class StandaloneTcpExample {

  private static final String ACCOUNT_ALIAS = "admin@gns.name"; // REPLACE THIS WITH YOUR ACCOUNT ALIAS
  private static final String PASSWORD = "password";
  private static GNSClientCommands client;
  private static GuidEntry accountGuid;

  public static void main(String[] args) throws IOException,
          InvalidKeySpecException, NoSuchAlgorithmException, ClientException,
          InvalidKeyException, SignatureException, Exception {
    
    InetSocketAddress address = ServerSelectDialog.selectServer();
    client = new GNSClientCommands(null);
    try {
      accountGuid = GuidUtils.lookupOrCreateAccountGuid(client, ACCOUNT_ALIAS, PASSWORD);
    } catch (Exception e) {
      System.out.println("Exception during accountGuid creation: " + e);
      System.exit(1);
    }
    System.out.println("Client connected to GNS at " + address.getHostName() + ":" + address.getPort());

    // Retrive the GUID using the account id
    String guid = client.lookupGuid(ACCOUNT_ALIAS);
    System.out.println("Retrieved GUID for " + ACCOUNT_ALIAS + ": " + guid);

    // Get the public key from the GNS
    PublicKey publicKey = client.publicKeyLookupFromGuid(guid);
    System.out.println("Retrieved public key: " + publicKey.toString());

    // Use the GuidEntry create an new record in the GNS
    client.fieldCreateOneElementList(accountGuid, "homestate", "Florida");
    System.out.println("Added location -> Florida record to the GNS for GUID " + accountGuid.getGuid());

    // Retrive that record from the GNS
    String result = client.fieldReadArrayFirstElement(accountGuid.getGuid(), "homestate", accountGuid);
    System.out.println("Result of read location: " + result);
    
     // Retrive that record from the GNS
    client.fieldReplace(accountGuid, "homestate", "Massachusetts");
    System.out.println("Changed location -> Massachusetts in the GNS for GUID " + accountGuid.getGuid());
    
     // Retrive that record from the GNS
    result = client.fieldReadArrayFirstElement(accountGuid.getGuid(), "homestate", accountGuid);
    System.out.println("Result of read location: " + result);
    
    System.exit(0);
  }
  
}
