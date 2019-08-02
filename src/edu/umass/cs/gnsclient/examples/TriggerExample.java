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
import edu.umass.cs.gnsclient.client.GNSCommand;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnscommon.AclAccessType;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.exceptions.client.AclException;
import edu.umass.cs.gnscommon.utils.RandomString;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.Arrays;

import edu.umass.cs.utils.Utils;
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
public class TriggerExample {

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

		client = new GNSClient();
		System.out.println("[Client connected to GNS]\n");

		try {
			System.out.println("// account GUID creation\n"
				+ "client.createAccount(" + ACCOUNT_NAME+")");
			client.execute(GNSCommand.createAccount(ACCOUNT_NAME));
			GUID = GuidUtils.getGUIDKeys(ACCOUNT_NAME);
			System.out.println("// created GUID " + GUID);
		} catch (Exception | Error e) {
			System.out.println("Exception during accountGuid creation: " + e);
			e.printStackTrace();
			System.exit(1);
		}

		// Create a JSON Object to initialize our guid record
		JSONObject json = new JSONObject(
			"{\"name\":\"me\",\"location\":\"work\"}").put("hello",new
			JSONObject().put("world", "people").put("planet", "pluto"));

		System.out.println("\n// Update GUID record\n"
			+ "client.update(GUID, record) // record=" + json);
		client.execute(GNSCommand.update(GUID, json));


		String notifiee = "notifiee", other = "other";
		GuidEntry notifeeGUID = null, otherGUID = null;
		// Create another GUID
		try {
			System.out.println("\n// another account GUID creation\n"
				+ "client.createAccount(" + notifiee + ")");
			client.execute(GNSCommand.createAccount(notifiee));
			notifeeGUID = GuidUtils.getGUIDKeys(notifiee);
			System.out.println("// created notifeeGUID" + notifeeGUID );

			DatagramSocket datagramSocket = null;
			datagramSocket = new DatagramSocket(0, InetAddress.getByName
				("localhost"));

			String location = "location";

			System.out.println("\n// adding trigger for field location \n"
			+ "client.addTrigger(GUID, notifieeGUID, location, datagramIP, " +
				"datagramPort)");
			client.execute(GNSCommand.addTrigger(GUID.getGuid(), notifeeGUID,
				Arrays.asList(location).toArray(new String[0]),
				datagramSocket.getLocalAddress().getHostAddress().toString(),
				datagramSocket.getLocalPort()));

			System.out.println("\n// updating GUID.location\n" +
			"client.fieldUpdate(GUID, location, \"home\")");
			client.execute(GNSCommand.fieldUpdate(GUID, location, "home"));

			System.out.println("\n// printing notification if any received on" +
				" datagram socket");
			byte[] buf = new byte[1024];
			DatagramPacket datagramPacket= new DatagramPacket(buf, buf.length);
			datagramSocket.receive(datagramPacket);
			System.out.println(new String(datagramPacket.getData()));

		} catch (Exception | Error e) {
			System.out.println("Exception during accountGuid creation: " + e);
			e.printStackTrace();
			System.exit(1);
		}


		System.out.println("\n// Example complete, gracefully closing the client\n"
				+ "client.close()");
		client.close();
	}
}
