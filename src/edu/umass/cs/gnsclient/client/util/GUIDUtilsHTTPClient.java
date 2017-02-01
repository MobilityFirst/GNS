package edu.umass.cs.gnsclient.client.util;

import edu.umass.cs.gnsclient.client.http.HttpClient;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.exceptions.client.DuplicateNameException;

import java.io.IOException;

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

    public static GuidEntry lookupGuidEntryFromDatabase(String gnsInstance, String name) {
      return KeyPairUtils.getGuidEntry(gnsInstance, name);
    }

    // Screaming for an interface
    public static GuidEntry lookupGuidEntryFromDatabase(HttpClient client, String name) {
      return lookupGuidEntryFromDatabase(client.getGNSProvider(), name);
    }
}
