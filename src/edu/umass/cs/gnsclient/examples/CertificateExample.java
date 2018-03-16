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

import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GNSCommand;
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
 * Note: This example assumes that the verification step (e.g., via email or a
 * third-party certificate) to verify an account GUID's human-readable name has
 * been disabled on the servers.
 *
 * @author arun, westy
 */
public class CertificateExample {

	private static String TEST_ACCOUNT_PASSWORD = "password";
	private static GNSClient client;
	private static GuidEntry GUID;

    private static String certificateFileName = "/home/tramteja/Desktop/testing/GNS/certificate_justgns.crt";
    private static String privateKeyFileName = "/home/tramteja/Desktop/testing/GNS/private_key.pe   m";

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
		client = new GNSClient();
		System.out.println("[Client connected to GNS]\n");

        try {
            GUID = GuidUtils.lookupOrCreateAccountGuidWithCertificate(client, certificateFileName, privateKeyFileName, TEST_ACCOUNT_PASSWORD, true);
            //GUID = GuidUtils.accountGuidCreateWithCertificate(client, TEST_ACCOUNT_PASSWORD, certificateFileName, privateKeyFileName);
        } catch(Exception e) {
            System.out.println("Exception during accountGuid creation: " + e);
            e.printStackTrace();
            System.exit(1);
        }

        
		// Delete created GUID
		// client.execute(GNSCommand.accountGuidRemove(guid));
		System.out.println("\n// GUID delete\n"
				+ "client.accountGuidRemove(GUID) // GUID=" + GUID);

		// Try read the entire record after deleting (expecting to fail)
		try {
			String resultString = client.execute(GNSCommand.accountGuidRemove(GUID))
					.getResultString();
		} catch (Exception e) {
			System.out.println("\n// non-existent GUID error (expected)\n"
					+ "client.read(GUID) // GUID= " + GUID + "\n  "
					+ e.getMessage());
		}

		client.close();
		System.out.println("\nclient.close() // test successful");
	}
}
