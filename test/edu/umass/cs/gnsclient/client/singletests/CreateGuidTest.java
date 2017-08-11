/* Copyright (c) 2015 University of Massachusetts
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
 * Initial developer(s): Westy, arun */
package edu.umass.cs.gnsclient.client.singletests;

import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.GNSCommand;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnscommon.ResponseCode;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.utils.RandomString;
import edu.umass.cs.gnsserver.utils.DefaultGNSTest;
import edu.umass.cs.utils.Utils;

import java.io.IOException;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * Basic test for the GNS using the UniversalTcpClient.
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class CreateGuidTest extends DefaultGNSTest {

	private static GNSClientCommands clientCommands;
	private static GuidEntry masterGuid;

	/**
   *
   */
	public CreateGuidTest() {

		if (clientCommands == null) {
			clientCommands = new GNSClientCommands(client);
			try {
				masterGuid = GuidUtils.getGUIDKeys(globalAccountName);
			} catch (Exception e) {
				Utils.failWithStackTrace("Exception when we were not expecting it: "
						+ e);
			}

		}
	}

	/**
   *
   */
	@Test
	public void test_01_CreateEntity() {
		String alias = "testGUID" + RandomString.randomString(12);
		GuidEntry guidEntry = null;
		try {
			guidEntry = clientCommands.guidCreate(masterGuid, alias);
		} catch (ClientException | IOException e) {
			Utils.failWithStackTrace("Exception while creating guid: " + e);
		}
		Assert.assertNotNull(guidEntry);
		Assert.assertEquals(alias, guidEntry.getEntityName());
		try {
			clientCommands.guidRemove(masterGuid, guidEntry.getGuid());
		} catch (ClientException | IOException e) {
			Utils.failWithStackTrace("Exception while creating guid: " + e);
		}
	}

	/**
 * 
 */
	@Test
	public void test_02_CreateSubGUIDofSubGUIDShouldFail() {
		String alias1 = "testGUID" + RandomString.randomString(12);
		String alias2 = "testGUID" + RandomString.randomString(12);
		GuidEntry guidEntry1 = null;
		try {
			client.execute(GNSCommand.createGUID(masterGuid, alias1));
			guidEntry1 = GuidUtils.getGUIDKeys(alias1);
			client.execute(GNSCommand.createGUID(guidEntry1, alias2));
		} catch (ClientException e) {
			// expected
			Assert.assertEquals(ResponseCode.BAD_ACCOUNT_ERROR, e.getCode());
			try {
				client.execute(GNSCommand.guidRemove(masterGuid,
						guidEntry1.getGuid()));
			} catch (ClientException | IOException e1) {
				Utils.failWithStackTrace("Exception while creating guid: " + e);
			}
			return;
		} catch (IOException e) {
			Utils.failWithStackTrace("Exception while creating guid: " + e);
		}
		// can not come here
		Assert.assertTrue(
				"Creating a subGUID of a subGUID did not fail as expected",
				false);
	}

	/**
	 * Tests creation, update, read, and removal of keyless GUIDs.
	 */
	public void test_03_CreateUpdateReadRemoveKeylessGUID() {

		String keyless1 = "keyless1";
		try {
			client.execute(GNSCommand.guidCreateKeyless(masterGuid, keyless1));
		} catch (ClientException | IOException e) {
			Utils.failWithStackTrace("Keyless GUID creation of " + keyless1
					+ " failed", e);
		}

		// Create a JSON Object to initialize our guid record
		JSONObject json = null;
		try {
			json = new JSONObject("{\"occupation\":\"busboy\","
					+ "\"friends\":[\"Joe\",\"Sam\",\"Billy\"],"
					+ "\"gibberish\":{\"meiny\":\"bloop\",\"einy\":\"floop\"},"
					+ "\"location\":\"work\",\"name\":\"frank\"}");
		} catch (JSONException e) {
			Utils.failWithStackTrace(
					"JSONException while creating update object", e);
		}

		/* Note; keyless GUIDs are fake GUIDs, i.e., they are not
		 * self-certifying hashes of a public key, but simply the HRN itself. */
		String keyless1GUID = null;
		try {
			keyless1GUID = client.execute(GNSCommand.lookupGUID(keyless1))
					.getResultString();
		} catch (ClientException | IOException e) {
			Utils.failWithStackTrace("Failed to look up keyless GUID", e);
		}
		try {
			client.execute(GNSCommand.update(keyless1GUID, json, masterGuid));
		} catch (ClientException | IOException e) {
			Utils.failWithStackTrace("Failed to update keyless GUID", e);
		}

		try {
			JSONObject readJSON = client.execute(
					GNSCommand.read(keyless1GUID, masterGuid))
					.getResultJSONObject();
			Assert.assertEquals(json.toString(), readJSON.toString());
		} catch (ClientException | IOException e) {
			Utils.failWithStackTrace("Failed to read keyless GUID", e);
		}
		try {
			client.execute(GNSCommand.guidRemove(masterGuid, keyless1GUID));
		} catch (ClientException | IOException e) {
			Utils.failWithStackTrace("Failed to remove keyless GUID"
					+ keyless1GUID, e);
		}
	}
}
