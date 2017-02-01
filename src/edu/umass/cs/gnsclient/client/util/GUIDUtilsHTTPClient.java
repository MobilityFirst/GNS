package edu.umass.cs.gnsclient.client.util;

import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.http.HttpClient;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.exceptions.client.DuplicateNameException;
import edu.umass.cs.gnscommon.exceptions.client.EncryptionException;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

/**
 * Created by kanantharamu on 1/31/17.
 */
public class GUIDUtilsHTTPClient {
    // Screaming for an interface...
    private static boolean guidExists(HttpClient client, GuidEntry guid) throws IOException {
      try {
        client.lookupGuidRecord(guid.getGuid());
      } catch (ClientException e) {
        return false;
      }
      return true;
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

    // Screaming for an interface
    public static GuidEntry lookupGuidEntryFromDatabase(HttpClient client, String name) {
      return GUIDUtilsGNSClient.lookupGuidEntryFromDatabase(client.getGNSProvider(), name);
    }

    public static GuidEntry lookupGuidEntryFromDatabase(GNSClientCommands client, String name) {
      return GUIDUtilsGNSClient.lookupGuidEntryFromDatabase(client.getGNSProvider(), name);
    }

    public static GuidEntry getGUIDKeys(String name) {
          return GUIDUtilsGNSClient.lookupGuidEntryFromDatabase(
                  GNSClient.getGNSProvider(), name);
      }

    public static GuidEntry lookupOrCreateGuidEntry(String alias, String gnsInstance)
            throws NoSuchAlgorithmException, EncryptionException {
      GuidEntry entry;
      if ((entry = GUIDUtilsGNSClient.lookupGuidEntryFromDatabase(gnsInstance, alias)) != null) {
        return entry;
      } else {
        return GUIDUtilsGNSClient.createAndSaveGuidEntry(alias, gnsInstance);
      }
    }

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

    public static GuidEntry lookupOrCreateAccountGuid(GNSClientCommands client, String name,
                                                        String password) throws Exception {
      return lookupOrCreateAccountGuid(client, name, password, false);
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

}
