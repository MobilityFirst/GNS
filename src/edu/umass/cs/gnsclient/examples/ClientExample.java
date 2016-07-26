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

import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;

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
 * This example creates an account GUID record, performs a few reads and writes
 * to its fields, and deletes the record.
 * <p>
 * Note: This example assumes that the verification step (e.g., via email) to
 * verify an account GUID's human-readable name has been disabled on the server
 * using the ENABLE_EMAIL_VERIFICATION=false option.
 * 
 * @author arun, westy
 */
public class ClientExample {

	// replace with your account alias
	private static String ACCOUNT_ALIAS = "admin@gns.name";
	private static GNSClientCommands client;
	private static GuidEntry guid;

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

		/* Create the client that connects to a default reconfigurator as
		 * specified in gigapaxos properties file. */
		client = new GNSClientCommands();
		System.out.println("[Client connected to GNS]\n");

		try {
			/**
			 * Create an account GUID if one doesn't already exists. The true
			 * flag makes it verbosely print out what it is doing. The password
			 * is for future use and is needed mainly if the keypair is
			 * generated on the server in order to retrieve the private key.
			 * lookupOrCreateAccountGuid "cheats" by bypassing email-based or
			 * other verification mechanisms using a shared secret between the
			 * server and the client.
			 * */
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
		JSONObject json = new JSONObject("{\"occupation\":\"busboy\","
				+ "\"friends\":[\"Joe\",\"Sam\",\"Billy\"],"
				+ "\"gibberish\":{\"meiny\":\"bloop\",\"einy\":\"floop\"},"
				+ "\"location\":\"work\",\"name\":\"frank\"}");

		// Write out the JSON Object
		client.update(guid, json);
		System.out.println("\n// record update\n"
				+ "client.update(GUID, record) // record=" + json);

		// and read the entire object back in
		JSONObject result = client.read(guid);
		System.out.println("client.read(GUID) -> " + result.toString());

		// Change a field
		client.update(guid, new JSONObject(
				"{\"occupation\":\"rocket scientist\"}"));
		System.out
				.println("\n// field update\n"
						+ "client.update(GUID, fieldKeyValue) // fieldKeyValue={\"occupation\":\"rocket scientist\"}");

		// and read the entire object back in
		result = client.read(guid);
		System.out.println("client.read(GUID) -> " + result.toString());

		// Add a field
		client.update(guid, new JSONObject("{\"ip address\":\"127.0.0.1\"}"));
		System.out
				.println("\n// field add\n"
						+ "client.update(GUID, fieldKeyValue) // fieldKeyValue= {\"ip address\":\"127.0.0.1\"}");

		// and read the entire object back in
		result = client.read(guid);
		System.out.println("client.read(GUID) -> " + result.toString());

		// Remove a field
		client.fieldRemove(guid.getGuid(), "gibberish", guid);
		System.out.println("\n// field remove\n"
				+ "client.fieldRemove(GUID, \"gibberish\")");

		// and read the entire object back in
		result = client.read(guid);
		System.out.println("client.read(GUID) -> " + result.toString());

		// Add some more stuff to read back
		JSONObject newJson = new JSONObject();
		JSONObject subJson = new JSONObject();
		subJson.put("sally", "red");
		subJson.put("sammy", "green");
		JSONObject subsubJson = new JSONObject();
		subsubJson.put("right", "seven");
		subsubJson.put("left", "eight");
		subJson.put("sally", subsubJson);
		newJson.put("flapjack", subJson);
		client.update(guid, newJson);
		System.out.println("\n// field add with JSON value\n"
				+ "client.update(GUID, fieldKeyValue) // fieldKeyValue="
				+ newJson);

		// Read a single field at the top level
		String resultString = client.fieldRead(guid, "flapjack");
		System.out.println("client.fieldRead(\"flapjack\") -> " + resultString);

		// Read a single field using dot notation
		resultString = client.fieldRead(guid, "flapjack.sally.right");
		System.out.println("\n// dotted field read\n"
				+ "client.fieldRead(GUID, \"flapjack.sally.right\") -> "
				+ resultString);

		// Update a field using dot notation
		JSONArray newValue = new JSONArray(
				Arrays.asList("One", "Ready", "Frap"));
		client.fieldUpdate(guid, "flapjack.sammy", newValue);
		System.out.println("\n// dotted field update\n"
				+ "client.fieldUpdate(GUID, \"flapjack.sammy\", " + newValue);

		// Read the same field using dot notation
		resultString = client.fieldRead(guid, "flapjack.sammy");
		System.out.println("client.fieldRead(GUID, \"flapjack.sammy\") -> "
				+ resultString);

		// Read two fields at a time
		resultString = client.fieldRead(guid,
				new ArrayList<String>(Arrays.asList("name", "occupation")));
		System.out.println("\n// multi-field read\n"
				+ "client.fieldRead(GUID, [\"name\",\"occupation\"] -> "
				+ resultString);

		// Read the entire object back in
		result = client.read(guid);
		System.out.println("\nclient.read(GUID) -> " + result.toString());

		
		// Delete created GUID
		client.accountGuidRemove(guid);
		System.out.println("\n// GUID delete\n"
				+ "client.accountGuidRemove(GUID) // GUID=" + guid);

		// Try read the entire record
		try {
			result = client.read(guid);
		} catch (Exception e) {
			System.out.println("\n// non-existent GUID error (expected)\n"
					+ "client.read(GUID) // GUID= " + guid + "\n  "
					+ e.getMessage());
		}

		client.close();
		System.out.println("\nclient.close() // test successful");
	}
}
