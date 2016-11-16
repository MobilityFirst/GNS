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
 *  Initial developer(s): Westy
 *
 */
package edu.umass.cs.gnsclient.client.singletests;


import edu.umass.cs.gnsclient.client.CommandUtils;
import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;

import java.security.PrivateKey;
import java.security.PublicKey;

import org.json.JSONObject;

import static org.junit.Assert.*;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.exceptions.client.EncryptionException;

import java.io.IOException;

/**
 * Signature functionality test for the GNS Tcp client.
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestSignatureTest {

  private static String ACCOUNT_ALIAS = "admin@gns.name"; // REPLACE THIS WITH YOUR ACCOUNT ALIAS
  private static final String privateKeyFile = "/Users/westy/pkcs8_key";
  private static GNSClientCommands client;
  private static GuidEntry guid;

  /**
   *
   * @throws EncryptionException
   */
  public TestSignatureTest() throws EncryptionException {
    if (client == null) {
       try {
        client = new GNSClientCommands();
        client.setForceCoordinatedReads(true);
      } catch (IOException e) {
        fail("Exception creating client: " + e);
      }
      // Retrive the GNSProtocol.GUID.toString() using the account id
      String guidString;
      try {
        guidString = client.lookupGuid(ACCOUNT_ALIAS);
      } catch (Exception e) {
        e.printStackTrace();
        return;
      }
      try {
        System.out.println("Retrieved GUID for " + ACCOUNT_ALIAS + ": " + guidString);
      } catch (Exception e) {
        e.printStackTrace();
        return;
      }
      PublicKey publicKey;
      try {
        // Get the public key from the GNS
        publicKey = client.publicKeyLookupFromGuid(guidString);
      } catch (Exception e) {
        e.printStackTrace();
        return;
      }
      PrivateKey privateKey;
      System.out.println("Retrieved public key: " + publicKey.toString());
      try {
        privateKey = KeyPairUtils.getPrivateKeyFromPKCS8File(privateKeyFile);
      } catch (Exception e) {
        e.printStackTrace();
        return;
      }
      System.out.println("Retrieved private key: " + privateKey.toString());
      guid = new GuidEntry(ACCOUNT_ALIAS, guidString, publicKey, privateKey);
      System.out.println("Created GUID entry: " + guid.toString());
    }
  }

  /**
   *
   */
  @Test
  public void test_01() {
    try {
      JSONObject command = CommandUtils.createAndSignCommand(CommandType.ReadArrayUnsigned, guid.getPrivateKey(),
              guid.getPublicKey(),
              GNSProtocol.GUID.toString(), guid.getGuid(), GNSProtocol.FIELD.toString(), "joe");
      System.out.println(command);
    } catch (Exception e) {
      fail("Exception when we were not expecting it: " + e);
    }
  }
}
