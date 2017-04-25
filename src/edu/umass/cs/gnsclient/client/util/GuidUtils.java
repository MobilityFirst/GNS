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

import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.GNSClientConfig;
import edu.umass.cs.gnsclient.client.GNSCommand;
import edu.umass.cs.gnsclient.client.http.HttpClient;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.exceptions.client.DuplicateNameException;
import edu.umass.cs.gnscommon.exceptions.client.EncryptionException;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.logging.Level;

/**
 * Utilities designed to make it easier to work with guids.
 *
 * @author arun, westy
 */
public class GuidUtils {

  //
  // Some of the code in here is screaming for an interface.
  //
  private static boolean guidExists(GNSClientCommands client, GuidEntry guid) throws IOException {
    try {
      client.lookupGuidRecord(guid.getGuid());
    } catch (ClientException e) {
      return false;
    }
    return true;
  }

  private static boolean guidExists(GNSClient client, GuidEntry guid) throws IOException {
    try {
      client.execute(GNSCommand.lookupGUID(guid.getGuid())).getResultJSONObject();
    } catch (ClientException e) {
      return false;
    }
    return true;
  }

  // Screaming for an interface...
  private static boolean guidExists(HttpClient client, GuidEntry guid) throws IOException {
    try {
      client.lookupGuidRecord(guid.getGuid());
    } catch (ClientException e) {
      return false;
    }
    return true;
  }

  /**
   * Creates and verifies an account GNSProtocol.GUID.toString(). Yes it cheats on verification.
   *
   * @param client
   * @param name
   * @param password
   * @return Created {@link GuidEntry}
   * @throws Exception
   */
  public static GuidEntry lookupOrCreateAccountGuid(GNSClientCommands client, String name,
          String password) throws Exception {
    return lookupOrCreateAccountGuid(client, name, password, false);
  }

  /**
   * Creates a GuidEntry associating an alias with a new key pair and stores
   * it in a local key database. This method will overwrite existing entries
   * if any for alias.
   *
   * @param alias
   * @param gnsInstance
   * @return {@link GuidEntry} created for {@code alias}.
   * @throws NoSuchAlgorithmException
   * @throws EncryptionException
   */
  private static GuidEntry generateAndSaveKeyPairForGuidAlias(String gnsInstance,
          String alias) throws NoSuchAlgorithmException, EncryptionException {
    return KeyPairUtils.generateAndSaveKeyPair(gnsInstance, alias);
  }

  /**
   * @param client
   * @param name
   * @param password
   * @return Refer {@link #lookupOrCreateAccountGuid(GNSClient, String, String, boolean)}.
   * @throws Exception
   */
  public static GuidEntry lookupOrCreateAccountGuid(GNSClient client,
          String name, String password) throws Exception {
    return lookupOrCreateAccountGuid(client, name, password, false);
  }

  /**
   * @param client
   * @param name
   * @param password
   * @param verbose
   * @return Created {@link GuidEntry} for {@code name}.
   * @throws Exception
   */
  public static GuidEntry lookupOrCreateAccountGuid(GNSClient client,
          String name, String password, boolean verbose) throws Exception {
    GuidEntry guid = lookupGuidEntryFromDatabase(client, name);
    if (guid == null || !guidExists(client, guid)) {
      if (guid == null) {
        if (verbose) {
          System.out.println("  Creating a new account GUID for " + name);
        }
        GNSClientConfig.getLogger().log(Level.INFO,
                "Creating a new account GUID for {0}",
                new Object[]{name});
        guid = generateAndSaveKeyPairForGuidAlias(client.getGNSProvider(), name);
      } else {
        if (verbose) {
          System.out.println("  Old account GUID " + guid
                  + " found locally is invalid, creating a new one.");
        }
        GNSClientConfig.getLogger().log(Level.INFO,
                " Old account GUID {0} found locally is invalid, creating a new one",
                new Object[]{name});
      }
      try {
        client.execute(GNSCommand.createAccount(name,
                password));
      } catch (DuplicateNameException e) {
        /*
         * Ignore duplicate name exception as it is most likely because we
         * ourselves successfully created it earlier. If the exception is because
         * someone else created the name, subsequent commands requiring signatures
         * will not work correctly.
         */
        if (verbose) {
          System.out.println("  Account GUID " + guid
                  + " aready exists on the server; "
                  + e.getMessage());
        }
      }
      if (verbose) {
        System.out.println("  Created account GUID " + guid);
      }
      return guid;
    } else {
      if (verbose) {
        System.out.println("Found account guid for "
                + guid.getEntityName() + " (" + guid.getGuid() + ")");
      }
      return guid;
    }
  }

  /**
   * @param client
   * @param name
   * @param password
   * @param verbose
   * @return Created {@link GuidEntry} for {@code name}.
   * @throws Exception
   */
  public static GuidEntry lookupOrCreateAccountGuid(GNSClientCommands client, String name, String password,
          boolean verbose) throws Exception {
    return lookupOrCreateAccountGuidInternal(client, name, password, false, verbose);
  }

  /**
   * @param client
   * @param name
   * @param password
   * @param verbose
   * @return Created {@link GuidEntry} for {@code name}.
   * @throws Exception
   */
  public static GuidEntry lookupOrCreateAccountGuidSecured(GNSClientCommands client, String name, String password,
          boolean verbose) throws Exception {
    return lookupOrCreateAccountGuidInternal(client, name, password, true, verbose);
  }

  private static GuidEntry lookupOrCreateAccountGuidInternal(GNSClientCommands client,
          String name, String password,
          boolean secured, boolean verbose) throws Exception {
    GuidEntry guid = lookupGuidEntryFromDatabase(client, name);
    // If we didn't find the guid or the entry in the database is obsolete we
    // create a new guid.
    if (guid == null || !guidExists(client, guid)) {
      if (verbose) {
        if (guid == null) {
          System.out.println("  Creating a new account GUID for " + name);
        } else {
          System.out.println("  Old account GUID " + guid + " found locally is invalid, creating a new one.");
        }
      }
      try {
        if (secured) {
          guid = client.accountGuidCreateSecure(name, password);
        } else {
          guid = client.accountGuidCreate(name, password);
        }
      } catch (DuplicateNameException e) {
        // ignore as it is most likely because of a seemingly failed creation operation that actually succeeded.
        System.out.println("  Account GUID " + guid + " aready exists on the server; " + e.getMessage());
      }
      if (verbose) {
        if (secured) {
          System.out.println("  Created and verified account GUID " + guid);
        } else {
          System.out.println("  Created account GUID " + guid);
        }
      }
      return guid;
    } else {
      if (verbose) {
        System.out.println("Found account guid for " + guid.getEntityName() + " (" + guid.getGuid() + ")");
      }
      return guid;
    }
  }

  /**
   * @param client
   * @param name
   * @param password
   * @return Refer {@link #lookupOrCreateAccountGuid(GNSClient, String, String, boolean)}.
   * @throws Exception
   */
  public static GuidEntry lookupOrCreateAccountGuid(HttpClient client,
          String name, String password) throws Exception {
    return lookupOrCreateAccountGuid(client, name, password, false);
  }

  /**
   * @param client
   * @param name
   * @param password
   * @param verbose
   * @return Created {@link GuidEntry} for {@code name}.
   * @throws Exception
   */
  public static GuidEntry lookupOrCreateAccountGuid(HttpClient client, String name, String password,
          boolean verbose) throws Exception {
    GuidEntry guid = lookupGuidEntryFromDatabase(client, name);
    // If we didn't find the guid or the entry in the database is obsolete we
    // create a new guid.
    if (guid == null || !guidExists(client, guid)) {
      if (verbose) {
        if (guid == null) {
          System.out.println("  Creating a new account GUID for " + name);
        } else {
          System.out.println("  Old account GUID " + guid + " found locally is invalid, creating a new one.");
        }
      }
      try {
        guid = client.accountGuidCreate(name, password);
      } catch (DuplicateNameException e) {
        // ignore as it is most likely because of a seemingly failed creation operation that actually succeeded.
        System.out.println("  Account GUID " + guid + " aready exists on the server; " + e.getMessage());
      }
      if (verbose) {
        System.out.println("  Created account GUID " + guid);
      }
      return guid;
    } else {
      if (verbose) {
        System.out.println("Found account guid for " + guid.getEntityName() + " (" + guid.getGuid() + ")");
      }
      return guid;
    }
  }

  /**
   * Creates and verifies an account GNSProtocol.GUID.toString().
   *
   * @param client
   * @param accountGuid
   * @param name
   * @return Created {@link GuidEntry} for {@code name}.
   * @throws Exception
   */
  public static GuidEntry lookupOrCreateGuid(GNSClientCommands client, GuidEntry accountGuid, String name)
          throws Exception {
    return lookupOrCreateGuid(client, accountGuid, name, false);
  }

  /**
   * Creates and verifies a subguid (created under an accountGuid)
   *
   * @param client
   * @param accountGuid
   * @param name
   * @param verbose
   * @return Created {@link GuidEntry} for {@code name}.
   * @throws Exception
   */
  public static GuidEntry lookupOrCreateGuid(GNSClientCommands client, GuidEntry accountGuid, String name,
          boolean verbose) throws Exception {
    GuidEntry guid = lookupGuidEntryFromDatabase(client, name);
    // If we didn't find the guid or the entry in the database is obsolete we
    // create a new guid.
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

  /**
   * Creates and verifies a subguid (created under an accountGuid).
   *
   * @param client
   * @param accountGuid
   * @param name
   * @return Created {@link GuidEntry} for {@code name}.
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * @throws java.io.IOException
   */
  public static GuidEntry lookupOrCreateGuid(GNSClient client, GuidEntry accountGuid, String name)
          throws ClientException, IOException {
    return lookupOrCreateGuid(client, accountGuid, name, false);
  }

  /**
   * Creates and verifies a subguid (created under an accountGuid).
   *
   * @param client
   * @param accountGuid
   * @param name
   * @param verbose
   * @return Created {@link GuidEntry} for {@code name}.
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * @throws java.io.IOException
   */
  public static GuidEntry lookupOrCreateGuid(GNSClient client, GuidEntry accountGuid, String name, boolean verbose)
          throws ClientException, IOException {
    GuidEntry guid = lookupGuidEntryFromDatabase(client, name);
    // If we didn't find the guid or the entry in the database is obsolete we
    // create a new guid.
    if (guid == null || !guidExists(client, guid)) {
      if (verbose) {
        if (guid == null) {
          System.out.println("Creating a new guid for " + name);
        } else {
          System.out.println("Old guid for " + name + " is invalid. Creating a new one.");
        }
      }
      client.execute(GNSCommand.guidCreate( accountGuid, name));
      guid = lookupGuidEntryFromDatabase(client, name);
      return guid;
    } else {
      if (verbose) {
        System.out.println("Found guid for " + guid.getEntityName() + " (" + guid.getGuid() + ")");
      }
      return guid;
    }
  }

  /**
   * Looks up the guid information associated with alias that is stored in the preferences.
   *
   * @param client
   * @param name
   * @return {@link GuidEntry} from local key database.
   */
  public static GuidEntry lookupGuidEntryFromDatabase(GNSClientCommands client, String name) {
    return GuidUtils.lookupGuidEntryFromDatabase(client.getGNSProvider(), name);
  }

	/**
	 * @param name
	 * @return GuidEntry retrieved from local database.
	 */
	public static GuidEntry getGUIDKeys(String name) {
		return GuidUtils.lookupGuidEntryFromDatabase(
				GNSClient.getGNSProvider(), name);
	}

  /**
   * @param gnsInstance
   * @param name
   * @return {@link GuidEntry} from local key database.
   */
  public static GuidEntry lookupGuidEntryFromDatabase(String gnsInstance, String name) {
    return KeyPairUtils.getGuidEntry(gnsInstance, name);
  }

  /**
   * @param client
   * @param name
   * @return {@link GuidEntry} from local key database.
   */
  public static GuidEntry lookupGuidEntryFromDatabase(GNSClient client, String name) {
    return KeyPairUtils.getGuidEntry(client.getGNSProvider(), name);
  }

  /**
   * Looks up the guid information associated with alias that is stored in the preferences.
   *
   * @param client
   * @param name
   * @return {@link GuidEntry} from local key database.
   */
  // Screaming for an interface
  public static GuidEntry lookupGuidEntryFromDatabase(HttpClient client, String name) {
    return GuidUtils.lookupGuidEntryFromDatabase(client.getGNSProvider(), name);
  }

  /**
   * Creates a GuidEntry associating an alias with a new key pair and stores
   * it in a local key database. This method will overwrite existing entries
   * if any for alias.
   *
   * @param alias
   * @param hostport
   * @return {@link GuidEntry} created for {@code alias}.
   * @throws NoSuchAlgorithmException
   * @throws EncryptionException
   */
  public static GuidEntry createAndSaveGuidEntry(String alias, String hostport)
          throws NoSuchAlgorithmException, EncryptionException {
    return generateAndSaveKeyPairForGuidAlias(hostport, alias);
  }

  /**
   * Finds a GuidEntry which associated with an alias or creates and stores it in the local preferences.
   *
   * @param alias
   * @param gnsInstance
   * @return {@link GuidEntry}
   * @throws NoSuchAlgorithmException
   * @throws EncryptionException
   */
  public static GuidEntry lookupOrCreateGuidEntry(String alias, String gnsInstance)
          throws NoSuchAlgorithmException, EncryptionException {
    GuidEntry entry;
    if ((entry = GuidUtils.lookupGuidEntryFromDatabase(gnsInstance, alias)) != null) {
      return entry;
    } else {
      return createAndSaveGuidEntry(alias, gnsInstance);
    }
  }
}
