
package edu.umass.cs.gnsclient.client.util;

import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.GNSClientConfig;
import edu.umass.cs.gnsclient.client.GNSCommand;
import edu.umass.cs.gnsclient.client.http.HttpClient;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.SharedGuidUtils;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.exceptions.client.DuplicateNameException;
import edu.umass.cs.gnscommon.exceptions.client.EncryptionException;

import java.io.IOException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.util.logging.Level;


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


  public static GuidEntry lookupOrCreateAccountGuid(GNSClientCommands client, String name,
          String password) throws Exception {
    return lookupOrCreateAccountGuid(client, name, password, false);
  }


  private static GuidEntry generateAndSaveKeyPairForGuidAlias(String gnsInstance,
          String alias) throws NoSuchAlgorithmException, EncryptionException {
    KeyPair keyPair = KeyPairGenerator.getInstance(GNSProtocol.RSA_ALGORITHM.toString())
            .generateKeyPair();
    String guid = SharedGuidUtils.createGuidStringFromPublicKey(keyPair
            .getPublic().getEncoded());
    // Squirrel this away now just in case the call below times out.
    KeyPairUtils.saveKeyPair(gnsInstance, alias, guid, keyPair);
    return new GuidEntry(alias, guid, keyPair.getPublic(),
            keyPair.getPrivate());
  }


  public static GuidEntry lookupOrCreateAccountGuid(GNSClient client,
          String name, String password) throws Exception {
    return lookupOrCreateAccountGuid(client, name, password, false);
  }


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


  public static GuidEntry lookupOrCreateAccountGuid(GNSClientCommands client, String name, String password,
          boolean verbose) throws Exception {
    return lookupOrCreateAccountGuidInternal(client, name, password, false, verbose);
  }


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


  public static GuidEntry lookupOrCreateAccountGuid(HttpClient client,
          String name, String password) throws Exception {
    return lookupOrCreateAccountGuid(client, name, password, false);
  }


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


  public static GuidEntry lookupOrCreateGuid(GNSClientCommands client, GuidEntry accountGuid, String name)
          throws Exception {
    return lookupOrCreateGuid(client, accountGuid, name, false);
  }


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


  public static GuidEntry lookupOrCreateGuid(GNSClient client, GuidEntry accountGuid, String name)
          throws ClientException, IOException {
    return lookupOrCreateGuid(client, accountGuid, name, false);
  }


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
      client.execute(GNSCommand.createGUID( accountGuid, name));
      guid = lookupGuidEntryFromDatabase(client, name);
      return guid;
    } else {
      if (verbose) {
        System.out.println("Found guid for " + guid.getEntityName() + " (" + guid.getGuid() + ")");
      }
      return guid;
    }
  }


  public static GuidEntry lookupGuidEntryFromDatabase(GNSClientCommands client, String name) {
    return GuidUtils.lookupGuidEntryFromDatabase(client.getGNSProvider(), name);
  }


	public static GuidEntry getGUIDKeys(String name) {
		return GuidUtils.lookupGuidEntryFromDatabase(
				GNSClient.getGNSProvider(), name);
	}


  public static GuidEntry lookupGuidEntryFromDatabase(String gnsInstance, String name) {
    return KeyPairUtils.getGuidEntry(gnsInstance, name);
  }


  public static GuidEntry lookupGuidEntryFromDatabase(GNSClient client, String name) {
    return KeyPairUtils.getGuidEntry(client.getGNSProvider(), name);
  }


  // Screaming for an interface
  public static GuidEntry lookupGuidEntryFromDatabase(HttpClient client, String name) {
    return GuidUtils.lookupGuidEntryFromDatabase(client.getGNSProvider(), name);
  }


  public static GuidEntry createAndSaveGuidEntry(String alias, String hostport)
          throws NoSuchAlgorithmException, EncryptionException {
    return generateAndSaveKeyPairForGuidAlias(hostport, alias);
  }


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
