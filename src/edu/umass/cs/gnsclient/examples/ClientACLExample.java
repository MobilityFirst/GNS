/* Copyright (c) 2016 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * Initial developer(s): Westy */
package edu.umass.cs.gnsclient.examples;

import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnscommon.AclAccessType;
import edu.umass.cs.gnscommon.GNSCommandProtocol;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.utils.RandomString;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.Arrays;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * This example creates an account GUID record and an additional GUID to demonstrate ACL commands in the GNS.
 * 
 * <p>
 * Note: This example assumes that the verification step (e.g., via email) to
 * verify an account GUID's human-readable name has been disabled on the server
 * using the ENABLE_EMAIL_VERIFICATION=false option.
 * 
 * @author arun, westy, mdews
 */
public class ClientACLExample {

  // replace with your account alias
  private static String ACCOUNT_ALIAS = "admin@gns.name";
  private static GNSClientCommands client;
  private static GuidEntry myGuid;
  private static GuidEntry phoneGuid;
  
  /**
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

    client = new GNSClientCommands();
    System.out.println("[Client connected to GNS]\n");

    try {
      System.out
          .println("// Account GUID creation\n"
              + "GuidUtils.lookupOrCreateAccountGuid(client, ACCOUNT_ALIAS,"
              + " \"password\", true)");
      myGuid = GuidUtils.lookupOrCreateAccountGuid(client, ACCOUNT_ALIAS,
          "password", true);
    } catch (Exception | Error e) {
      System.out.println("Exception during accountGuid creation: " + e);
      e.printStackTrace();
      System.exit(1);
    }

    // Create a JSON Object to initialize our guid record
    JSONObject json = new JSONObject("{\"name\":\"me\",\"location\":\"work\"}");

    // Write out the JSON Object
    client.update(myGuid, json);
    System.out.println("\n// Record update\n"
        + "client.update(GUID, record) // record=" + json);
    
    // Remove default read access from myGuid
    System.out.println("\n// Remove default read access from myGuid");
    client.aclRemove(AclAccessType.READ_WHITELIST, myGuid, 
        GNSCommandProtocol.ALL_FIELDS, GNSCommandProtocol.ALL_GUIDS);

    // Create phoneGuid
    System.out.println("\n// Create phoneGuid");
    phoneGuid = client.guidCreate(myGuid, "phone" + RandomString.randomString(12));
    
    // Give phoneGuid read access to fields in myGuid
    // If we had not removed the default read access from myGuid this step would be unnecessary
    System.out.println("\n// Give phoneGuid read access to fields in myGuid");
    client.aclAdd(AclAccessType.READ_WHITELIST, myGuid, GNSCommandProtocol.ALL_FIELDS, phoneGuid.getGuid());
    
    // Verify that phoneGuid can read the fields in myGuid
    JSONObject result = client.read(myGuid.getGuid(), phoneGuid);
    System.out.println("\n// phoneGuid read from myGuid: " + result);
     
    // Allow phoneGuid to write to the location field of myGuid
    System.out.println("\n// Give phoneGuid write access to \"location\" field of myGuid");
    client.aclAdd(AclAccessType.WRITE_WHITELIST, myGuid, "location", phoneGuid.getGuid());
    
    // As phoneGuid, update the location field on myGuid
    System.out.println("\n// Use phoneGuid to update \"location\" field of myGuid");
    client.fieldUpdate(myGuid.getGuid(), "location", "home", phoneGuid);
    result = client.read(myGuid.getGuid(), phoneGuid);
    System.out.println("// Updated myGuid: " + result);

    // Remove phoneGuid from ACL
    System.out.println("\n// Remove phoneGuid from read and write list");
    client.aclRemove(AclAccessType.READ_WHITELIST, myGuid, GNSCommandProtocol.ALL_FIELDS, phoneGuid.getGuid());
    client.aclRemove(AclAccessType.WRITE_WHITELIST, myGuid, "location", phoneGuid.getGuid());
    // Verify
    try {
      result = client.read(myGuid.getGuid(), phoneGuid);
      System.out.println("\n// phoneGuid read from myGuid (unexpected): " + result);
      System.out.println("\nFAIL");
      client.close();
      System.exit(1);
    } catch (Exception e) {
      System.out.println("\n// phoneGuid failed to read from myGuid (expected)");
    }
    try {
      System.out.println("\n// Use phoneGuid to update \"location\" field of myGuid");
      client.fieldUpdate(myGuid.getGuid(), "location", "vacation", phoneGuid);
      System.out.println("\nFAIL");
      client.close();
      System.exit(1);
    } catch (Exception e) {
      System.out.println("\n// Using phoneGuid to update \"location\" field of myGuid failed (expected)");
    }
    
    client.close();
    System.out.println("\nclient.close() // example complete");
  }
}
