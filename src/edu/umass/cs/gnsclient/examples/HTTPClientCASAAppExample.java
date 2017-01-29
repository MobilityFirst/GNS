
package edu.umass.cs.gnsclient.examples;

import edu.umass.cs.gnsclient.client.http.HttpClient;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnscommon.AclAccessType;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.exceptions.client.DuplicateNameException;
import org.json.JSONObject;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;


public class HTTPClientCASAAppExample {

  // replace with your account alias
  private static String ourAppAlias = "exampleAppAccount@gns.name";
  private static HttpClient client;
  private static GuidEntry guidEntry;
  private static final String MASTER_ALIAS = "example_ACS_master_alias@gns.name";


  public static void main(String[] args) throws IOException,
          InvalidKeySpecException, NoSuchAlgorithmException, ClientException,
          InvalidKeyException, SignatureException, Exception {

    // Create the client
    client = new HttpClient("127.0.0.1", 8080);
    // First thing we do is create an account to simulate the master account guid
    // that the ACS is using - don't worry about this for now.
    createSimulatedMasterAccount();
    //
    // Now on to the app commands.
    //
    // Check to see if our guid already exists
    String lookupResult = null;
    try {
      lookupResult = client.lookupGuid(ourAppAlias);
    } catch (ClientException e) {
      // The normal result if the alias doesn't exist
    } catch (IOException e) {
      System.out.println("\nProblem during lookupGuid: " + e);
      System.exit(1);
    }
    // If it exists we exit
    if (lookupResult != null) {
      System.out.println("\nGuid for " + ourAppAlias + " already exists. This can happen if you "
              + "run this class twice. Exiting.");
      System.exit(1);
    }
    System.out.println("\n// guid lookup\n"
           + "lookupGuid didn't find " + ourAppAlias + ". This is good.");
    
    //
    // Now we actually create the account guid. 
    // Note that accountGuidCreate looks up or creates a private public key pair for us.
    try {
      guidEntry = client.accountGuidCreate(ourAppAlias, "samplePassword");
    } catch (DuplicateNameException e) {
      // ignore as it is most likely because of a seemingly failed creation operation that actually succeeded.
      System.out.println("Account GUID " + guidEntry + " aready exists on the server; continuing.");
    }
    System.out.println("\n// guid create\n" 
            + "Created guid for " + ourAppAlias + ": " + guidEntry.getGuid());

    //
    // Update the ACLs.
    // Next we remove the default read access that the GNS gives for all fields.
    client.aclRemove(AclAccessType.READ_WHITELIST, guidEntry,
            GNSProtocol.ENTIRE_RECORD.toString(), GNSProtocol.EVERYONE.toString());
    System.out.println("\n// acl update\n"
            + "Removed default read whitelist for " + ourAppAlias);
    
    // Look up the guid of the simulate master account we created above.
    // In the running architecture this is created by the ACS.
    String masterGuid;
    // Give the master ACS guid (simulated) access to all fields of our guid for reading and writing.
    masterGuid = client.lookupGuid(ourAppAlias);
    client.aclAdd(AclAccessType.READ_WHITELIST, guidEntry, GNSProtocol.ENTIRE_RECORD.toString(), masterGuid);
    client.aclAdd(AclAccessType.WRITE_WHITELIST, guidEntry, GNSProtocol.ENTIRE_RECORD.toString(), masterGuid);
    System.out.println("\n// acl update part two\n" 
            + "Added read and write access for " + masterGuid + " to " + ourAppAlias);

    // Now do some writing and reading writing.
    // Create a JSON Object to initialize our guid record
    JSONObject json = new JSONObject("{\"occupation\":\"busboy\","
            + "\"friends\":[\"Joe\",\"Sam\",\"Billy\"],"
            + "\"gibberish\":{\"meiny\":\"bloop\",\"einy\":\"floop\"},"
            + "\"location\":\"work\",\"name\":\"frank\"}");

    // Write out the JSON Object.
    client.update(guidEntry, json);
    System.out.println("\n// record update\n"
            + "client.update(guidEntry, record) // record=" + json);

    // And read the entire object back in.
    JSONObject result = client.read(guidEntry);
    System.out.println("client.read(guidEntry) -> " + result.toString());

    client.close();
    System.out.println("\nclient.close() // test successful");
  }

  private static void createSimulatedMasterAccount() {
    try {
      guidEntry = GuidUtils.lookupOrCreateAccountGuid(client, MASTER_ALIAS, "samplePassword");
    } catch (Exception e) {
      System.out.println("\nProblem creating simulated master account: " + e);
      System.exit(1);
    }
  }
}
