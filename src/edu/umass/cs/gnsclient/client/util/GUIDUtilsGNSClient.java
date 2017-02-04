package edu.umass.cs.gnsclient.client.util;

import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GNSClientConfig;
import edu.umass.cs.gnsclient.client.GNSCommand;
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

/**
 * Created by kanantharamu on 1/31/17.
 */
public class GUIDUtilsGNSClient {
    private static boolean guidExists(GNSClient client, GuidEntry guid) throws IOException {
      try {
        client.execute(GNSCommand.lookupGUID(guid.getGuid())).getResultJSONObject();
      } catch (ClientException e) {
        return false;
      }
      return true;
    }

    static GuidEntry generateAndSaveKeyPairForGuidAlias(String gnsInstance,
                                                        String alias) throws NoSuchAlgorithmException, EncryptionException {


      KeyPair keyPair  =    IOSKeyPairUtils.generateKeyPair();
      String guid = SharedGuidUtils.createGuidStringFromPublicKey(keyPair
              .getPublic().getEncoded());
      // Squirrel this away now just in case the call below times out.
      IOSKeyPairUtils.saveKeyPair(gnsInstance, alias, guid, keyPair);
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

    public static GuidEntry lookupGuidEntryFromDatabase(GNSClient client, String name) {
      return IOSKeyPairUtils.getGuidEntry(client.getGNSProvider(), name);
    }

  public static GuidEntry createAndSaveGuidEntry(String alias, String hostport)
          throws NoSuchAlgorithmException, EncryptionException {
    return generateAndSaveKeyPairForGuidAlias(hostport, alias);
  }

  public static GuidEntry lookupGuidEntryFromDatabase(String gnsInstance, String name) {
    return IOSKeyPairUtils.getGuidEntry(gnsInstance, name);
  }
}
