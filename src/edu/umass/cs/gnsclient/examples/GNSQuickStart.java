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

import edu.umass.cs.gnsclient.client.GuidEntry;
import edu.umass.cs.gnsclient.client.UniversalTcpClientExtended;
import edu.umass.cs.gnscommon.utils.ByteUtils;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.gnsclient.client.util.ServerSelectDialog;
import edu.umass.cs.gnsclient.exceptions.GnsException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;

/**
 * OUT OF DATE: THIS WILL BE UPDATED SHORTLY TO REFLECT THE LATEST CLIENT CHANGES.
 * 
 * This code example will help you get started using the GNS Java client library to access the GNS. 
 * In this example we will use an account you have created to write and read back some 
 * information in the GNS.
  
 * It uses the UniversalTcpClient class to issue HTTP requests the GNS service. 
 * Some requests will return a value as a Java String. Requests that
 * don't expect a response to be returned will return the string +OK+. A bad 
 * response is indicated by the string +NO+. See the GnsProtocol class for more details.
 * 
 * You will need to create an account using your email at the site http://gns.name. 
 * Your email will be your account name. A public / private key pair will be generated when you register. The
 * GNS will store and use you public key for authentication. You will need to download the private key and
 * store it in a known location which you will also need to specify in the code below.
 * 
 * @author westy
 */
public class GNSQuickStart {
  
  // *** REPLACE THIS WITH THE VALUE YOU USED TO CREATE YOUR ACCOUNT ***
  // The email you used to create your GNS account guid
  private static final String accountId = "david@westy.org";
  //
  // *** REPLACE THIS WITH THE LOCATION OF YOUR PRIVATE KEY FILE ***
  // The location of your private key file. 
  // A private key was created when you created your GNS account. 
  // If you used the CLI to create your account you can use the command
  // key_savePKCS8 to create the file.
  // Alternatively:
  // If you use the online account creation you will need to download they key file.
  // It is a PEM file that needs to be converted to a PKCS#8 format file.
  // Use this command to convert it to a standard PKCS#8 format file:
  // > openssl pkcs8 -topk8 -inform PEM -outform DER -in <input file>  -nocrypt > <output file>
  private static final String privateKeyFile = "/Users/Westy/test_key";

  public static void main(String[] args) throws IOException, 
          InvalidKeySpecException, NoSuchAlgorithmException, GnsException,
          InvalidKeyException, SignatureException, Exception {
    
    // A convenience function that pops up a GUI for picking which GNS server you want to use.
    InetSocketAddress address = ServerSelectDialog.selectServer();
    // Create a new client object
    UniversalTcpClientExtended client = new UniversalTcpClientExtended(address.getHostName(), address.getPort(), true);
    System.out.println("Client connected to GNS at " + address.getHostName() + ":" + address.getPort());
    
    // Retrive the GUID using the account id
    String guid = client.lookupGuid(accountId);
    System.out.println("Retrieved GUID for " + accountId + ": " + guid);

    // Get the public key from the GNS
    PublicKey publicKey = client.publicKeyLookupFromGuid(guid);
    System.out.println("Retrieved public key: " + publicKey.toString());

    // Load the private key from a file
    PrivateKey privateKey = KeyPairUtils.getPrivateKeyFromPKCS8File(privateKeyFile);
    System.out.println("Retrieved private key: " + ByteUtils.toHex(privateKey.getEncoded()).toString());

    // Create a GuidEntry
    GuidEntry accountGuid = new GuidEntry(accountId, guid, publicKey, privateKey);
    System.out.println("Created GUID entry: " + accountGuid.toString());
    
    // Use the GuidEntry create a new record in the GNS
    client.fieldCreateOneElementList(accountGuid, "homestate", "Florida");
    System.out.println("Added location -> Florida record to the GNS for GUID " + accountGuid.getGuid());

    // Retrieve that record from the GNS
    String result = client.fieldReadArrayFirstElement(accountGuid.getGuid(), "homestate", accountGuid);
    System.out.println("Result of read location: " + result);
    
    // Update the value of the field
    client.fieldReplace(accountGuid, "homestate", "Massachusetts");
    System.out.println("Changed location -> Massachusetts in the GNS for GUID " + accountGuid.getGuid());
    
    // Retrieve that record from the GNS again
    result = client.fieldReadArrayFirstElement(accountGuid.getGuid(), "homestate", accountGuid);
    System.out.println("Result of read location: " + result);
    
    System.exit(0);
  }
}
