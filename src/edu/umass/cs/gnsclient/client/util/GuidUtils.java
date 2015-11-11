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
package edu.umass.cs.gnsclient.client.util;

import edu.umass.cs.gnsclient.client.GNSClientInterface;
import edu.umass.cs.gnscommon.utils.ThreadUtils;
import edu.umass.cs.gnscommon.utils.ByteUtils;
import edu.umass.cs.gnscommon.GnsProtocol;
import edu.umass.cs.gnsclient.client.GuidEntry;
import edu.umass.cs.gnsclient.exceptions.GnsException;
import edu.umass.cs.gnsclient.exceptions.GnsInvalidGuidException;
import edu.umass.cs.gnsclient.exceptions.GnsVerificationException;
import java.io.IOException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/**
 *
 * @author westy
 */
public class GuidUtils {

  public static final String JUNIT_TEST_TAG = "JUNIT";
  // this is so we can mimic the verification code the server is generting
  // AKA we're cheating... if the SECRET changes on the server side
  // you'll need to change it here as well
  private static final String SECRET = "AN4pNmLGcGQGKwtaxFFOKG05yLlX0sXRye9a3awdQd2aNZ5P1ZBdpdy98Za3qcE" + "o0u6BXRBZBrcH8r2NSbqpOoWfvcxeSC7wSiOiVHN7fW0eFotdFz0fiKjHj3h0ri";
  private static final int VERIFICATION_CODE_LENGTH = 3; // Six hex characters

  private static boolean guidExists(GNSClientInterface client, GuidEntry guid) throws IOException {
    try {
      client.lookupGuidRecord(guid.getGuid());
    } catch (GnsException e) {
      return false;
    }
    return true;
  }

  public static GuidEntry registerGuidWithTestTag(GNSClientInterface client, GuidEntry masterGuid, String entityName) throws Exception {
    return registerGuidWithTag(client, masterGuid, entityName, JUNIT_TEST_TAG);
  }

  /**
   * Creates and verifies an account GUID. Yes it cheats on verification.
   *
   * @param client
   * @param name
   * @return
   * @throws Exception
   */
  public static GuidEntry lookupOrCreateAccountGuid(GNSClientInterface client, String name,
          String password) throws Exception {
    return lookupOrCreateAccountGuid(client, name, password, false);
  }

  public static GuidEntry lookupOrCreateAccountGuid(GNSClientInterface client, String name, String password,
          boolean verbose) throws Exception {
    GuidEntry guid = lookupGuidEntryFromPreferences(client, name);
    if (guid == null || !guidExists(client, guid)) {
      if (verbose) {
        if (guid == null) {
          System.out.println("Creating a new account guid for " + name);
        } else {
          System.out.println("Old account guid for " + name + " is invalid. Creating a new one.");
        }
      }
      guid = client.accountGuidCreate(name, password);
      // Since we're cheating here we're going to catch already verified errors which means
      // someone on the server probably turned off verification for testing purposes
      // but we'll rethrow everything else
      try {
        client.accountGuidVerify(guid, createVerificationCode(name));
      } catch (GnsVerificationException e) {
        // a bit of a hack here that depends on someone not changing that error message
        if (!e.getMessage().endsWith("Account already verified")) {
          throw e;
        } else {
          System.out.println("Caught and ignored \"Account already verified\" error for " + name);
        }
      }
      if (verbose) {
        System.out.println("Created and verified account guid for " + guid.getEntityName() + " (" + guid.getGuid() + ")");
      }
      return guid;
    } else {
      if (verbose) {
        System.out.println("Found account guid for " + guid.getEntityName() + " (" + guid.getGuid() + ")");
      }
      return guid;
    }
  }

  private static String createVerificationCode(String name) {
    return ByteUtils.toHex(Arrays.copyOf(SHA1HashFunction.getInstance().hash(name + SECRET), VERIFICATION_CODE_LENGTH));
  }

  /**
   * Creates and verifies an account GUID. Yes it cheats on verification.
   *
   * @param client
   * @param name
   * @return
   * @throws Exception
   */
  public static GuidEntry lookupOrCreateGuid(GNSClientInterface client, GuidEntry accountGuid, String name) throws Exception {
    return lookupOrCreateGuid(client, accountGuid, name, false);
  }

  /**
   * Creates and verifies an account GUID. Yes it cheats on verification.
   *
   * @param client
   * @param name
   * @return
   * @throws Exception
   */
  public static GuidEntry lookupOrCreateGuid(GNSClientInterface client, GuidEntry accountGuid, String name, boolean verbose) throws Exception {
    GuidEntry guid = lookupGuidEntryFromPreferences(client, name);
    if (guid == null || !guidExists(client, guid)) {
      if (verbose) {
        if (guid == null) {
          System.out.println("Creating a new guid for " + name);
        } else {
          System.out.println("Old guid for " + name + " is invalid. Creating a new one.");
        }
      }
      guid = client.guidCreate(accountGuid, name);
      return guid;
    } else {
      if (verbose) {
        System.out.println("Found guid for " + guid.getEntityName() + " (" + guid.getGuid() + ")");
      }
      return guid;
    }
  }

  public static GuidEntry registerGuidWithTag(GNSClientInterface client, GuidEntry masterGuid, String entityName, String tagName) throws Exception {
    GuidEntry entry = client.guidCreate(masterGuid, entityName);
    try {
      client.addTag(entry, tagName);
    } catch (GnsInvalidGuidException e) {
      ThreadUtils.sleep(100);
      client.addTag(entry, tagName);
    }
    return entry;
  }

  /**
   * Looks up the guid information associated with alias that is stored in the preferences.
   *
   * @param client
   * @param name
   * @return
   */
  public static GuidEntry lookupGuidEntryFromPreferences(GNSClientInterface client, String name) {
    return lookupGuidEntryFromPreferences(client.getGnsRemoteHost(), client.getGnsRemotePort(), name);
  }

  public static GuidEntry lookupGuidEntryFromPreferences(String host, int port, String name) {
    return KeyPairUtils.getGuidEntry(host + ":" + port, name);
  }

  /**
   * Creates a GuidEntry which associates an alias with a new guid and key pair. Stores the
   * whole thing in the local preferences.
   *
   * @param alias
   * @param host
   * @param port
   * @return
   * @throws NoSuchAlgorithmException
   */
  public static GuidEntry createAndSaveGuidEntry(String alias, String host, int port) throws NoSuchAlgorithmException {
    KeyPair keyPair = KeyPairGenerator.getInstance(GnsProtocol.RSA_ALGORITHM).generateKeyPair();
    String guid = GuidUtils.createGuidFromPublicKey(keyPair.getPublic().getEncoded());
    KeyPairUtils.saveKeyPair(host + ":" + port, alias, guid, keyPair);
    return new GuidEntry(alias, guid, keyPair.getPublic(), keyPair.getPrivate());
  }

  /**
   * Finds a GuidEntry which associated with an alias or creates and stores it in the local preferences.
   *
   * @param alias
   * @param host
   * @param port
   * @return
   * @throws NoSuchAlgorithmException
   */
  public static GuidEntry lookupOrCreateGuidEntry(String alias, String host, int port) throws NoSuchAlgorithmException {
    GuidEntry entry;
    if ((entry = lookupGuidEntryFromPreferences(host, port, alias)) != null) {
      return entry;
    } else {
      return createAndSaveGuidEntry(alias, host, port);
    }
  }

  /**
   * Uses a hash function to generate a GUID from a public key.
   * This code is duplicated in server so if you
   * change it you should change it there as well.
   *
   * @param keyBytes
   * @return
   */
  public static String createGuidFromPublicKey(byte[] keyBytes) {
    byte[] publicKeyDigest = SHA1HashFunction.getInstance().hash(keyBytes);
    return ByteUtils.toHex(publicKeyDigest);
  }

}
