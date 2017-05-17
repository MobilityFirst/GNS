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
import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.GNSCommand;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnscommon.AclAccessType;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.exceptions.client.AclException;
import edu.umass.cs.gnscommon.utils.RandomString;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import org.json.JSONObject;

/**
 * This example creates an account GUID and sub-GUIDs and demonstrates how ACL
 * commands work in the GNS.
 * 
 * <p>
 * Note: This example assumes that the verification step (e.g., via email or a
 * third-party certificate) to verify an account GUID has been disabled on the
 * servers.
 * 
 * @author arun, westy, mdews
 */
public class ClientACLExample {

	// replace with your account alias
	private static String ACCOUNT_NAME = "user@gns.name";
	private static GNSClient client;
	private static GuidEntry GUID;
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
			System.out.println("// account GUID creation\n"
					+ "client.execute(" + ACCOUNT_NAME+")");
			client.execute(GNSCommand.createAccount(ACCOUNT_NAME));
			GUID = GuidUtils.getGUIDKeys(ACCOUNT_NAME);
		} catch (Exception | Error e) {
			System.out.println("Exception during accountGuid creation: " + e);
			e.printStackTrace();
			System.exit(1);
		}

		// Create a JSON Object to initialize our guid record
		JSONObject json = new JSONObject(
				"{\"name\":\"me\",\"location\":\"work\"}");

		// Write out the JSON Object
		client.execute(GNSCommand.update(GUID, json));
		System.out.println("\n// Update guid record\n"
				+ "client.update(guid, record) // record=" + json);

		// Remove default read access from guid
		client.execute(GNSCommand.aclRemove(AclAccessType.READ_WHITELIST, GUID,
				GNSProtocol.ENTIRE_RECORD.toString(),
				GNSProtocol.ALL_GUIDS.toString()));
		System.out
				.println("\n// Remove default read access from guid\n"
						+ "client.aclRemove(READ_WHITELIST, guid, ALL_FIELDS, ALL_GUIDS)");

		// Create phoneGuid
		// First we create an alias for the phoneGuid
		String phoneAlias = "phone" + RandomString.randomString(12);
		// Create a sub guid under our guid account
		client.execute(GNSCommand.guidCreate(GUID, phoneAlias));
		// Get the GuidEntry from the local database
		phoneGuid = GuidUtils.getGUIDKeys(phoneAlias);

		System.out.println("\n// Create phoneGuid\n"
				+ "client.guidCreate(guid, phoneAlias) // phoneAlias="
				+ phoneAlias);

		// Give phoneGuid read access to fields in guid
		// If we had not removed the default read access from guid this step
		// would be unnecessary
		client.execute(GNSCommand.aclAdd(AclAccessType.READ_WHITELIST, GUID,
				GNSProtocol.ENTIRE_RECORD.toString(), phoneGuid.getGuid()));
		JSONObject result = client.execute(
				GNSCommand.read(GUID.getGuid(), phoneGuid))
				.getResultJSONObject();
		System.out
				.println("\n// Give phoneGuid read access to fields in guid and read guid entry as phoneGuid\n"
						+ "client.aclAdd(READ_WHITELIST, guid, ALL_FIELDS, phoneGuid)\n"
						+ "client.read(guid, phoneGuid) -> " + result);

		// Allow phoneGuid to write to the location field of guid
		client.execute(GNSCommand.aclAdd(AclAccessType.WRITE_WHITELIST, GUID,
				"location", phoneGuid.getGuid()));
		System.out
				.println("\n// Give phoneGuid write access to \"location\" field of guid\n"
						+ "client.aclAdd(WRITE_WHITELIST, guid, \"location\", phoneGuid)");

		// As phoneGuid, update the location field on guid
		client.execute(GNSCommand.fieldUpdate(GUID.getGuid(), "location",
				"home", phoneGuid));
		String field_result = client.execute(
				GNSCommand.fieldRead(GUID.getGuid(), "location", phoneGuid))
				.getResultString();
		System.out
				.println("\n// Use phoneGuid to update \"location\" field of guid\n"
						+ "client.fieldUpdate(guid, \"location\", \"home\", phoneGuid)\n"
						+ "client.fieldRead(guid.getGuid(), \"location\", phoneGuid) -> "
						+ field_result);

		// Remove phoneGuid from ACL
		client.execute(GNSCommand.aclRemove(AclAccessType.READ_WHITELIST, GUID,
				GNSProtocol.ENTIRE_RECORD.toString(), phoneGuid.getGuid()));
		client.execute(GNSCommand.aclRemove(AclAccessType.WRITE_WHITELIST,
				GUID, "location", phoneGuid.getGuid()));
		System.out
				.println("\n// Remove phoneGuid from guid's read and write whitelists \n"
						+ "client.aclRemove(READ_WHITELIST, guid, ALL_FIELDS, phoneGuid))\n"
						+ "client.aclRemove(WRITE_WHITELIST, guid, \"location\", phoneGuid);");

		// Verify phoneGuid can't read guid (exception expected)
		try {
			System.out
					.println("\n// Attempting to read from guid using phoneGuid (failure expected)\n"
							+ "client.read(guid, phoneGuid)");
			result = client.execute(GNSCommand.read(GUID.getGuid(), phoneGuid))
					.getResultJSONObject();
			System.out
					.println("SOMETHING WENT WRONG. An exception should have been thrown. Terminating.");
			client.close();
			System.exit(1);
		} catch (AclException e) {
			System.out
					.println("// client.read failed as expected with the following AclException:\n"
							+ e.getMessage());
		}

		// Verify phoneGuid can't write to "location" field of guid (exception
		// expected)
		try {
			System.out
					.println("\n// Attempting to update \"location\" field of guid using phoneGuid (failure expected)\n"
							+ "client.fieldUpdate(guid.getGuid(), \"location\", \"vacation\", phoneGuid)");
			client.execute(GNSCommand.fieldUpdate(GUID.getGuid(), "location",
					"vacation", phoneGuid));
			System.out
					.println("\nSOMETHING WENT WRONG. An exception should have been thrown. Terminating.");
			client.close();
			System.exit(1);
		} catch (AclException e) {
			System.out
					.println("// client.fieldUpdate failed as expected with the following AclException:\n"
							+ e.getMessage());
		}

		System.out
				.println("\n// Example complete, gracefully closing the client\n"
						+ "client.close()");
		client.close();
	}
}
