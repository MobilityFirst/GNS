
package edu.umass.cs.gnsclient.examples;

import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.GNSCommand;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnscommon.AclAccessType;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.exceptions.client.AclException;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.utils.RandomString;
import org.json.JSONObject;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;


public class ClientACLExample {

  // replace with your account alias
  private static String ACCOUNT_ALIAS = "admin@gns.name";
  private static GNSClient client;
  private static GuidEntry guid;
  private static GuidEntry phoneGuid;
  

  public static void main(String[] args) throws IOException,
      InvalidKeySpecException, NoSuchAlgorithmException, ClientException,
      InvalidKeyException, SignatureException, Exception {

    client = new GNSClientCommands();
    System.out.println("[Client connected to GNS]\n");

    try {
      System.out
          .println("// account GUID creation\n"
              + "GuidUtils.lookupOrCreateAccountGuid(client, ACCOUNT_ALIAS,"
              + " \"password\", true)");
      guid = GuidUtils.lookupOrCreateAccountGuid(client, ACCOUNT_ALIAS,
          "password", true);
    } catch (Exception | Error e) {
      System.out.println("Exception during accountGuid creation: " + e);
      e.printStackTrace();
      System.exit(1);
    }

    // Create a JSON Object to initialize our guid record
    JSONObject json = new JSONObject("{\"name\":\"me\",\"location\":\"work\"}");

    // Write out the JSON Object
    client.execute(GNSCommand.update(guid, json));
    System.out.println("\n// Update guid record\n"
        + "client.update(guid, record) // record=" + json);
    
    // Remove default read access from guid
    client.execute(GNSCommand.aclRemove(AclAccessType.READ_WHITELIST, guid, 
        GNSProtocol.ENTIRE_RECORD.toString(), GNSProtocol.ALL_GUIDS.toString()));
    System.out.println("\n// Remove default read access from guid\n"
        + "client.aclRemove(READ_WHITELIST, guid, ALL_FIELDS, ALL_GUIDS)");
    
    // Create phoneGuid
    // First we create an alias for the phoneGuid
    String phoneAlias = "phone" + RandomString.randomString(12);
    // Create a sub guid under our guid account
    client.execute(GNSCommand.createGUID( guid, phoneAlias));
    // Get the GuidEntry from the local database
    phoneGuid = GuidUtils.lookupOrCreateGuidEntry(phoneAlias, client.getGNSProvider());

    System.out.println("\n// Create phoneGuid\n"
        + "client.createGuid(guid, phoneAlias) // phoneAlias=" + phoneAlias);
    
    // Give phoneGuid read access to fields in guid
    // If we had not removed the default read access from guid this step would be unnecessary
    client.execute(GNSCommand.aclAdd(AclAccessType.READ_WHITELIST, guid, GNSProtocol.ENTIRE_RECORD.toString(), phoneGuid.getGuid()));
    JSONObject result = client.execute(GNSCommand.read(guid.getGuid(), phoneGuid)).getResultJSONObject();
    System.out.println("\n// Give phoneGuid read access to fields in guid and read guid entry as phoneGuid\n"
        + "client.aclAdd(READ_WHITELIST, guid, ALL_FIELDS, phoneGuid)\n"
        + "client.read(guid, phoneGuid) -> " + result);
     
    // Allow phoneGuid to write to the location field of guid
    client.execute(GNSCommand.aclAdd(AclAccessType.WRITE_WHITELIST, guid, "location", phoneGuid.getGuid()));
    System.out.println("\n// Give phoneGuid write access to \"location\" field of guid\n"
        + "client.aclAdd(WRITE_WHITELIST, guid, \"location\", phoneGuid)");
    
    // As phoneGuid, update the location field on guid   
    client.execute(GNSCommand.fieldUpdate(guid.getGuid(), "location", "home", phoneGuid));
    String field_result = client.execute(GNSCommand.fieldRead(guid.getGuid(), "location", phoneGuid)).getResultString();
    System.out.println("\n// Use phoneGuid to update \"location\" field of guid\n"
        + "client.fieldUpdate(guid, \"location\", \"home\", phoneGuid)\n"
        + "client.fieldRead(guid.getGuid(), \"location\", phoneGuid) -> " + field_result);

    // Remove phoneGuid from ACL
    client.execute(GNSCommand.aclRemove(AclAccessType.READ_WHITELIST, guid, GNSProtocol.ENTIRE_RECORD.toString(), phoneGuid.getGuid()));
    client.execute(GNSCommand.aclRemove(AclAccessType.WRITE_WHITELIST, guid, "location", phoneGuid.getGuid()));
    System.out.println("\n// Remove phoneGuid from guid's read and write whitelists \n"
        + "client.aclRemove(READ_WHITELIST, guid, ALL_FIELDS, phoneGuid))\n"
        + "client.aclRemove(WRITE_WHITELIST, guid, \"location\", phoneGuid);");
    
    // Verify phoneGuid can't read guid (exception expected)
    try {
      System.out.println("\n// Attempting to read from guid using phoneGuid (failure expected)\n"
          + "client.read(guid, phoneGuid)");
      result = client.execute(GNSCommand.read(guid.getGuid(), phoneGuid)).getResultJSONObject();
      System.out.println("SOMETHING WENT WRONG. An exception should have been thrown. Terminating.");
      client.close();
      System.exit(1);
    } catch (AclException e) {
      System.out.println("// client.read failed as expected with the following AclException:\n"
          + e.getMessage());
    }
    
    // Verify phoneGuid can't write to "location" field of guid (exception expected) 
    try {
      System.out.println("\n// Attempting to update \"location\" field of guid using phoneGuid (failure expected)\n"
          + "client.fieldUpdate(guid.getGuid(), \"location\", \"vacation\", phoneGuid)");
      client.execute(GNSCommand.fieldUpdate(guid.getGuid(), "location", "vacation", phoneGuid));
      System.out.println("\nSOMETHING WENT WRONG. An exception should have been thrown. Terminating.");
      client.close();
      System.exit(1);
    } catch (AclException e) {
      System.out.println("// client.fieldUpdate failed as expected with the following AclException:\n"
          + e.getMessage());
    }
    
    System.out.println("\n// Example complete, gracefully closing the client\n"
        + "client.close()");
    client.close();
  }
}
