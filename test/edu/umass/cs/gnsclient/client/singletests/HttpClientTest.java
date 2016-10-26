/*
 * Copyright (C) 2016
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gnsclient.client.singletests;

import edu.umass.cs.gnsclient.client.http.HttpClient;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnscommon.AclAccessType;
import edu.umass.cs.gnscommon.GNSCommandProtocol;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.utils.RandomString;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import org.json.JSONException;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;

/**
 *
 * @author westy
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class HttpClientTest {

  private static final String ACCOUNT_ALIAS = "test@gns.name"; // REPLACE THIS WITH YOUR ACCOUNT ALIAS
  private static final String PASSWORD = "password";

  private HttpClient client;

  private static GuidEntry masterGuid;

  public HttpClientTest() {
    if (client == null) {
      client = new HttpClient("127.0.0.1", 8080);
      try {
        masterGuid = GuidUtils.lookupOrCreateAccountGuid(client, ACCOUNT_ALIAS, PASSWORD, true);

      } catch (Exception e) {
        fail("Exception while creating master guid: " + e);
      }
    }
  }

  /**
   *
   */
  @Test
  public void test_900_LookupGuid() {
    try {
      assertEquals(masterGuid.getGuid(), client.lookupGuid(ACCOUNT_ALIAS));
    } catch (IOException | ClientException e) {
      fail("Exception in LookupGuid: " + e);
    }
  }

  private static GuidEntry httpOneEntry;
  private static GuidEntry httpTwoEntry;

  /**
   *
   */
  @Test
  public void test_901_CreateGuids() {
    try {
      httpOneEntry = client.guidCreate(masterGuid, "httpOneEntry" + RandomString.randomString(6));
      httpTwoEntry = client.guidCreate(masterGuid, "httpTwoEntry" + RandomString.randomString(6));
      System.out.println("Created: " + httpOneEntry);
      System.out.println("Created: " + httpTwoEntry);
    } catch (IOException | ClientException | NoSuchAlgorithmException e) {
      failWithStackTrace("Exception in CreateFields: " + e);
    }
  }

  @Test
  public void test_902_RemoveACL() {
    try {
      // remove default read acces for this test
      client.aclRemove(AclAccessType.READ_WHITELIST, httpOneEntry,
              GNSCommandProtocol.ENTIRE_RECORD, GNSCommandProtocol.ALL_GUIDS);
    } catch (IOException | ClientException e) {
      failWithStackTrace("Exception in RemoveACL: " + e);
    }
  }

  @Test
  public void test_910_UpdateFields() {
    try {
      client.fieldUpdate(httpOneEntry.getGuid(), "environment", "work", httpOneEntry);
      client.fieldUpdate(httpOneEntry.getGuid(), "ssn", "000-00-0000", httpOneEntry);
      client.fieldUpdate(httpOneEntry.getGuid(), "password", "666flapJack", httpOneEntry);
      client.fieldUpdate(httpOneEntry.getGuid(), "address", "100 Hinkledinkle Drive", httpOneEntry);
    } catch (IOException | ClientException | JSONException e) {
      failWithStackTrace("Exception in UpdateFields: " + e);
    }
  }

  @Test
  public void test_911_CheckFields() {
    try {
      // read my own field
      assertEquals("work",
              client.fieldRead(httpOneEntry.getGuid(), "environment", httpOneEntry));
      // read another field
      assertEquals("000-00-0000",
              client.fieldRead(httpOneEntry.getGuid(), "ssn", httpOneEntry));
      // read another field
      assertEquals("666flapJack",
              client.fieldRead(httpOneEntry.getGuid(), "password", httpOneEntry));
    } catch (IOException | ClientException e) {
      failWithStackTrace("Exception in CheckFields: " + e);
    }
  }

  @Test
  public void test_913_CheckFieldsFail() {
    try {
      try {
        String result = client.fieldRead(httpOneEntry.getGuid(), "environment", httpTwoEntry);
        failWithStackTrace("Result of read of httpOneEntry's environment by httpTwoEntry is " + result
                + " which is wrong because it should have been rejected.");
      } catch (ClientException e) {
      } catch (IOException e) {
        failWithStackTrace("Exception during read of westy's environment by sam: " + e);
      }
    } catch (Exception e) {
      failWithStackTrace("Exception in CheckFieldsFail: " + e);
    }
  }

  @Test
  public void test_920_ACLAdd() {
    try {
      System.out.println("Using:" + httpOneEntry);
      System.out.println("Using:" + httpTwoEntry);
      try {
        client.aclAdd(AclAccessType.READ_WHITELIST, httpOneEntry, "environment",
                httpTwoEntry.getGuid());
      } catch (IOException | ClientException e) {
        fail("Exception adding Sam to Westy's readlist: " + e);
        e.printStackTrace();
      }
    } catch (Exception e) {
      failWithStackTrace("Exception in ACLAdd: " + e);
    }
  }

  @Test
  public void test_921_CheckAccess() {
    try {
      try {
        assertEquals("work",
                client.fieldRead(httpOneEntry.getGuid(), "environment", httpTwoEntry));
      } catch (IOException | ClientException e) {
        failWithStackTrace("Exception while Sam reading Westy's field: " + e);
      }
    } catch (Exception e) {
      failWithStackTrace("Exception in CheckAccess: " + e);
    }
  }

  private static final void failWithStackTrace(String message, Exception... e) {
    if (e != null && e.length > 0) {
      e[0].printStackTrace();
    }
    org.junit.Assert.fail(message);
  }
}
