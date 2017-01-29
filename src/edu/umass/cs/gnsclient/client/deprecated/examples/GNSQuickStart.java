
package edu.umass.cs.gnsclient.client.deprecated.examples;

import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import javax.xml.bind.DatatypeConverter;


public class GNSQuickStart {

  // *** REPLACE THIS WITH THE VALUE YOU USED TO CREATE YOUR ACCOUNT ***
  // The email you used to create your GNS account guid
  private static final String accountId = "support@gns.name";
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
          InvalidKeySpecException, NoSuchAlgorithmException, ClientException,
          InvalidKeyException, SignatureException, Exception {

    // Create a new client object
    GNSClientCommands client = new GNSClientCommands(null);
    System.out.println("Client connected to GNS");

    // Retrive the GUID using the account id
    String guid = client.lookupGuid(accountId);
    System.out.println("Retrieved GUID for " + accountId + ": " + guid);

    // Get the public key from the GNS
    PublicKey publicKey = client.publicKeyLookupFromGuid(guid);
    System.out.println("Retrieved public key: " + publicKey.toString());

    // Load the private key from a file
    PrivateKey privateKey = KeyPairUtils.getPrivateKeyFromPKCS8File(privateKeyFile);
    System.out.println("Retrieved private key: " + DatatypeConverter.printHexBinary(privateKey.getEncoded()));

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
