
package edu.umass.cs.gnsclient.client.deprecated.examples;

import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.util.BasicGuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import org.json.JSONArray;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;


public class ContextAwareGroupGuidExample {

  private static final String ACCOUNT_ALIAS = "admin@gns.name"; // REPLACE THIS WITH YOUR ACCOUNT ALIAS
  private static GNSClientCommands client;
  private static GuidEntry masterGuid;
  private static final String fieldName = "contextAwareExampleField";
  private static GuidEntry groupOneGuidEntry;
  private static GuidEntry groupTwoGuidEntry;


  public static void main(String[] args) throws IOException,
          InvalidKeySpecException, NoSuchAlgorithmException, ClientException,
          InvalidKeyException, SignatureException, Exception {

    // BOILER PLATE FOR RUNNING AN EXAMPLE
    // Start the client
    client = new GNSClientCommands(null);
    try {
      // Create the account guid using your email address and password = "password"
      masterGuid = lookupOrCreateAccountGuid(client, ACCOUNT_ALIAS, "password");
    } catch (Exception e) {
      System.out.println("Exception during accountGuid creation: " + e);
      System.exit(1);
    }
    System.out.println("Client connected to GNS.");

    // THE INTERESTING STUFF STARTS HERE
    // Create 5 guids each of which have the field using our fieldname with a value of 25
    for (int cnt = 0; cnt < 5; cnt++) {
      GuidEntry guidEntry = client.guidCreate(masterGuid, "valueField-" + cnt);
      client.fieldUpdate(guidEntry, fieldName, 25);
    }

    //FIXME: THIS CODE IS OBSOLETE. THE INTERFACE HAS CHANGED.
    // Create a guid for the first group
    groupOneGuidEntry = client.guidCreate(masterGuid, "contextAware > 20 Group");
    // Set up the group context with a query
    String query = "~" + fieldName + " : {$gt: 20}";
    // Note that the zero for the interval means that we will never get stale results (not a 
    // good idea to do in production code! The default is 60 (seconds))
    JSONArray result = client.selectSetupGroupQuery(masterGuid, groupOneGuidEntry.getGuid(), query, 0);

    // Show the values from the guids that we got back
    System.out.println("Members of " + groupOneGuidEntry.getEntityName() + ":");
    showFieldValuesInGuids(result, fieldName);

    //FIXME: THIS CODE IS OBSOLETE. THE INTERFACE HAS CHANGED.
    // Create a second group guid
    groupTwoGuidEntry = client.guidCreate(masterGuid, "contextAware = 0 Group");
    // Set up a second group with a different query
    query = "~" + fieldName + " : 0";
    // Note that the zero for the interval means that we will never get stale results (not a 
    // good idea to do in production code!  The default is 60 (seconds))
    JSONArray resultTwo = client.selectSetupGroupQuery(masterGuid, groupTwoGuidEntry.getGuid(), query, 0);

    // Show the values from the guids that we got back
    System.out.println("Members of " + groupTwoGuidEntry.getEntityName() + ": (should be empty)");
    showFieldValuesInGuids(resultTwo, fieldName);

    // Now we lookup the values of the first group again
    JSONArray resultThree = client.selectLookupGroupQuery(groupOneGuidEntry.getGuid());
    System.out.println("Members of " + groupOneGuidEntry.getEntityName() + ": (should still be 5 of them)");
    showFieldValuesInGuids(resultThree, fieldName);

    // THIS IS WHERE IT GETS INTERESTING
    // And change the values of all but one of them
    System.out.println("Changing 4 of the 5 guids to have a their " + fieldName + " field's value be 0");
    for (int i = 0; i < result.length() - 1; i++) {
      BasicGuidEntry guidInfo = new BasicGuidEntry(client.lookupGuidRecord(result.getString(i)));
      GuidEntry entry = GuidUtils.lookupGuidEntryFromDatabase(client, guidInfo.getEntityName());
      System.out.println("Changing value of " + fieldName + " field in " + entry.getEntityName() + " to 0");
      client.fieldUpdate(entry, fieldName, 0);
    }

    // Now we lookup the values of the first group again - showing that there is only one guid in the group
    JSONArray resultFour = client.selectLookupGroupQuery(groupOneGuidEntry.getGuid());
    System.out.println("Members of " + groupOneGuidEntry.getEntityName() + ": (should only be one of them)");
    showFieldValuesInGuids(resultFour, fieldName);

    // Now we lookup the values of the second group again - this one now has 4 guids
    JSONArray resultFive = client.selectLookupGroupQuery(groupTwoGuidEntry.getGuid());
    System.out.println("Members of " + groupTwoGuidEntry.getEntityName() + ": (should be 4 of them)");
    showFieldValuesInGuids(resultFive, fieldName);

    // So we can run this again without duplicate name/guid errors.
    cleanupAllGuids();

    System.exit(0);
  }

  //
  // private helper methods
  //

  private static void showFieldValuesInGuids(JSONArray guids, String field) throws Exception {
    for (int i = 0; i < guids.length(); i++) {
      BasicGuidEntry guidInfo = new BasicGuidEntry(client.lookupGuidRecord(guids.getString(i)));
      GuidEntry entry = GuidUtils.lookupGuidEntryFromDatabase(client, guidInfo.getEntityName());
      String value = client.fieldRead(entry, field);
      System.out.println(guids.get(i).toString() + ": " + field + " -> " + value);
    }
  }


  private static void cleanupAllGuids() throws Exception {
    // Remove the two group guids
    client.guidRemove(masterGuid, groupOneGuidEntry.getGuid());
    client.guidRemove(masterGuid, groupTwoGuidEntry.getGuid());
    // Remove all the value holding guids
    String query = "~" + fieldName + " : {$exists: true}";
    JSONArray result = client.selectQuery(query);
    for (int i = 0; i < result.length(); i++) {
      BasicGuidEntry guidInfo = new BasicGuidEntry(client.lookupGuidRecord(result.getString(i)));
      GuidEntry guidEntry = GuidUtils.lookupGuidEntryFromDatabase(client, guidInfo.getEntityName());
      client.guidRemove(masterGuid, guidEntry.getGuid());
    }
  }


  private static GuidEntry lookupOrCreateAccountGuid(GNSClientCommands client,
          String name, String password) throws Exception {
    GuidEntry guidEntry = KeyPairUtils.getGuidEntry(client.getGNSProvider(), name);
    if (guidEntry == null || !guidExists(client, guidEntry)) { // also handle case where it has been deleted from database 
      guidEntry = client.accountGuidCreateSecure(name, password);
      //guidEntry = client.accountGuidCreate(name, password);
      //client.accountGuidVerify(guidEntry, createVerificationCode(name));
      return guidEntry;
    } else {
      return guidEntry;
    }
  }

  private static boolean guidExists(GNSClientCommands client, GuidEntry guidEntry)
          throws IOException {
    try {
      client.lookupGuidRecord(guidEntry.getGuid());
    } catch (ClientException e) {
      return false;
    }
    return true;
  }

//  private static final int VERIFICATION_CODE_LENGTH = 3; // Three hex characters
//  // this is so we can mimic the verification code the server is generating
//  // AKA we're cheating... if the SECRET changes on the server side 
//  // you'll need to change it here as well
//  private static final String SECRET = Config.getGlobalString(GNSClientConfig.GNSCC.VERIFICATION_SECRET);
//
//  private static String createVerificationCode(String name) {
//    return ByteUtils.toHex(Arrays.copyOf(SHA1HashFunction.getInstance().hash(name + SECRET), VERIFICATION_CODE_LENGTH));
//  }

}
