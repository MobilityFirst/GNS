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
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.utils.RandomString;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import org.json.JSONException;
import org.junit.Test;
import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;
import org.junit.Assert;

/**
 *
 * @author westy
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SimpleHttpClientTest {

  private static final String ACCOUNT_ALIAS = "test@gns.name"; // REPLACE THIS WITH YOUR ACCOUNT ALIAS
  private static final String PASSWORD = "password";

  private HttpClient client;

  private static GuidEntry masterGuid;

  public SimpleHttpClientTest() {
    if (client == null) {
      client = new HttpClient("127.0.0.1", 8080);
    }
  }

  @Test
  public void test_900_Http_CreateAccountGuid() {
    try {
      masterGuid = GuidUtils.lookupOrCreateAccountGuid(client, ACCOUNT_ALIAS, PASSWORD, true);

    } catch (Exception e) {
      failWithStackTrace("Exception while creating master guid: ", e);
    }
  }

  /**
   *
   */
  @Test
  public void test_901_Http_LookupGuid() {
    try {
      Assert.assertEquals(masterGuid.getGuid(), client.lookupGuid(ACCOUNT_ALIAS));
    } catch (IOException | ClientException e) {
      failWithStackTrace("Exception in LookupGuid: ", e);
    }
  }

  private static GuidEntry httpOneEntry;
  private static GuidEntry httpTwoEntry;

  /**
   *
   */
  @Test
  public void test_902_Http_CreateGuids() {
    try {
      httpOneEntry = client.guidCreate(masterGuid, "httpOneEntry" + RandomString.randomString(6));
      httpTwoEntry = client.guidCreate(masterGuid, "httpTwoEntry" + RandomString.randomString(6));
      System.out.println("Created: " + httpOneEntry);
      System.out.println("Created: " + httpTwoEntry);
    } catch (IOException | ClientException | NoSuchAlgorithmException e) {
      failWithStackTrace("Exception in Http_CreateFields: ", e);
    }
  }

//  @Test
//  public void test_903_Http_RemoveACL() {
//    try {
//      // remove default read acces for this test
//      client.aclRemove(AclAccessType.READ_WHITELIST, httpOneEntry,
//              GNSProtocol.ENTIRE_RECORD.toString(), GNSProtocol.ALL_GUIDS.toString());
//    } catch (IOException | ClientException e) {
//      failWithStackTrace("Exception in Http_RemoveACL: ", e);
//    }
//  }

  @Test
  public void test_904_Http_UpdateFields() {
    try {
      client.fieldUpdate(httpOneEntry.getGuid(), "environment", "work", httpOneEntry);
    } catch (IOException | ClientException | JSONException e) {
      failWithStackTrace("Exception in Http_UpdateFields: ", e);
    }
  }

  private static final void failWithStackTrace(String message, Exception... e) {
    if (e != null && e.length > 0) {
      e[0].printStackTrace();
    }
    org.junit.Assert.fail(message);
  }
}
