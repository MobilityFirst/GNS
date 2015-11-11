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

import edu.umass.cs.gnsclient.client.BasicGuidEntry;
import edu.umass.cs.gnsclient.client.BasicUniversalTcpClient;
import edu.umass.cs.gnsclient.client.GuidEntry;
import edu.umass.cs.gnscommon.utils.ByteUtils;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.gnsclient.client.util.SHA1HashFunction;
import edu.umass.cs.gnsclient.client.util.ServerSelectDialog;
import edu.umass.cs.gnsclient.exceptions.GnsException;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import org.json.JSONArray;

/**
 * In this example we show how to create and use context aware group guids.
 *
 * The context aware group guid functionality allows one to create a group guid using
 * a query. The GNS will automatically maintain the membership of this group guid using
 * the query. Queries are specified using a syntax similar to that used by MongoDB. See
 * <link>https://gns.name/wiki/index.php?title=Query_Syntax</link> for a description of the
 * query syntax.
 * <p>
 * The main method in this class will create five example guids that have a field containing
 * a value of 25. We then create two context aware group guids that have are set up
 * with queries that look for that same field having a value of greater than 20 or zero
 * respectively. We will then retrieve those guid records and change the value of the
 * field in question to be zero. Then we will show how the two different context aware
 * group guids will retrieve the guids that correspond to their differeing queries.
 * <p>
 * The <code>BasicUniversalTcpClient</code> class contains two methods for creating and
 * looking up the value of a context aware group guid:
 * <p>
 * <code>JSONArray selectSetupGroupQuery(String guid, String query, int interval)</code>  - given
 * a guid and a query string this initializes the guid to automatically maintain it as
 * a context group guid. The interval specifies when a new lookup will get a fresh result.
 * This method also returns a JSONArray containing guids of records that match the
 * query.
 * <p>
 * <code>JSONArray selectLookupGroupQuery(String guid)</code>  - retrieves the current value of the
 * group guid. The result might be stale if the value was updated less than the time
 * interval specified in the setup call. The results as a JSONArray containing guids
 * of records that match the query.
 * <p>
 * Note: This example cheats during account guid creation in that it creates the account
 * guid and then uses the known secret to verify the account instead of making the user
 * verify the account manually deal with the private key.
 *
 * @author westy
 */
public class ContextAwareGroupGuidExample {

  private static final String ACCOUNT_ALIAS = "westy@cs.umass.edu"; // REPLACE THIS WITH YOUR ACCOUNT ALIAS
  private static BasicUniversalTcpClient client;
  private static GuidEntry masterGuid;
  private static final String fieldName = "contextAwareExampleField";
  private static GuidEntry groupOneGuidEntry;
  private static GuidEntry groupTwoGuidEntry;

  public static void main(String[] args) throws IOException,
          InvalidKeySpecException, NoSuchAlgorithmException, GnsException,
          InvalidKeyException, SignatureException, Exception {

    // BOILER PLATE FOR RUNNING AN EXAMPLE
    // Bring up the server selection dialog
    InetSocketAddress address = ServerSelectDialog.selectServer();
    // Start the client
    client = new BasicUniversalTcpClient(address.getHostName(), address.getPort());
    try {
      // Create the account guid using your email address and password = "password"
      masterGuid = lookupOrCreateAccountGuid(client, ACCOUNT_ALIAS, "password");
    } catch (Exception e) {
      System.out.println("Exception during accountGuid creation: " + e);
      System.exit(1);
    }
    System.out.println("Client connected to GNS at " + address.getHostName() + ":" + address.getPort());

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
      GuidEntry entry = GuidUtils.lookupGuidEntryFromPreferences(client, guidInfo.getEntityName());
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
  /**
   * Prints out the values of fields name field in the guids given in the guids list.
   *
   * @param guids
   * @param field
   * @throws Exception
   */
  private static void showFieldValuesInGuids(JSONArray guids, String field) throws Exception {
    for (int i = 0; i < guids.length(); i++) {
      BasicGuidEntry guidInfo = new BasicGuidEntry(client.lookupGuidRecord(guids.getString(i)));
      GuidEntry entry = GuidUtils.lookupGuidEntryFromPreferences(client, guidInfo.getEntityName());
      String value = client.fieldRead(entry, field);
      System.out.println(guids.get(i).toString() + ": " + field + " -> " + value);
    }
  }

  /**
   * Removes all the sub guids we use in this example.
   *
   * @throws Exception
   */
  private static void cleanupAllGuids() throws Exception {
    // Remove the two group guids
    client.guidRemove(masterGuid, groupOneGuidEntry.getGuid());
    client.guidRemove(masterGuid, groupTwoGuidEntry.getGuid());
    // Remove all the value holding guids
    String query = "~" + fieldName + " : {$exists: true}";
    JSONArray result = client.selectQuery(query);
    for (int i = 0; i < result.length(); i++) {
      BasicGuidEntry guidInfo = new BasicGuidEntry(client.lookupGuidRecord(result.getString(i)));
      GuidEntry guidEntry = GuidUtils.lookupGuidEntryFromPreferences(client, guidInfo.getEntityName());
      client.guidRemove(masterGuid, guidEntry.getGuid());
    }
  }

  /**
   * Creates and verifies an account GUID. Yes it cheats on verification
   * using a backdoor built into the GNS server.
   *
   * @param client
   * @param name
   * @return
   * @throws Exception
   */
  private static GuidEntry lookupOrCreateAccountGuid(BasicUniversalTcpClient client,
          String name, String password) throws Exception {
    GuidEntry guidEntry = KeyPairUtils.getGuidEntry(client.getGnsRemoteHost() + ":" + client.getGnsRemotePort(), name);
    if (guidEntry == null || !guidExists(client, guidEntry)) { // also handle case where it has been deleted from database
      guidEntry = client.accountGuidCreate(name, password);
      client.accountGuidVerify(guidEntry, createVerificationCode(name));
      return guidEntry;
    } else {
      return guidEntry;
    }
  }

  private static boolean guidExists(BasicUniversalTcpClient client, GuidEntry guidEntry)
          throws IOException {
    try {
      client.lookupGuidRecord(guidEntry.getGuid());
    } catch (GnsException e) {
      return false;
    }
    return true;
  }

  private static final int VERIFICATION_CODE_LENGTH = 3; // Six hex characters
  // this is so we can mimic the verification code the server is generting
  // AKA we're cheating... if the SECRET changes on the server side 
  // you'll need to change it here as well
  private static final String SECRET = "AN4pNmLGcGQGKwtaxFFOKG05yLlX0sXRye9a3awdQd2aNZ5P1ZBdpdy98Za3qcE"
          + "o0u6BXRBZBrcH8r2NSbqpOoWfvcxeSC7wSiOiVHN7fW0eFotdFz0fiKjHj3h0ri";

  private static String createVerificationCode(String name) {
    return ByteUtils.toHex(Arrays.copyOf(SHA1HashFunction.getInstance().hash(name + SECRET), VERIFICATION_CODE_LENGTH));
  }

}
