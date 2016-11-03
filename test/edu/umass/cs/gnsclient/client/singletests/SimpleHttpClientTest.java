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
import edu.umass.cs.gnsclient.client.util.JSONUtils;
import edu.umass.cs.gnsclient.jsonassert.JSONAssert;
import edu.umass.cs.gnsclient.jsonassert.JSONCompareMode;
import edu.umass.cs.gnscommon.AclAccessType;
import edu.umass.cs.gnscommon.GNSCommandProtocol;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.utils.RandomString;
import java.awt.geom.Point2D;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import static org.junit.Assert.assertEquals;
import org.junit.Test;
import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

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
      try {
        masterGuid = GuidUtils.lookupOrCreateAccountGuid(client, ACCOUNT_ALIAS, PASSWORD, true);

      } catch (Exception e) {
        failWithStackTrace("Exception while creating master guid: " + e);
      }
    }
  }

  /**
   *
   */
  @Test
  public void test_900_Http_LookupGuid() {
    try {
      assertEquals(masterGuid.getGuid(), client.lookupGuid(ACCOUNT_ALIAS));
    } catch (IOException | ClientException e) {
      failWithStackTrace("Exception in LookupGuid: " + e);
    }
  }

//  private static GuidEntry httpOneEntry;
//  private static GuidEntry httpTwoEntry;
//
//  /**
//   *
//   */
//  @Test
//  public void test_901_Http_CreateGuids() {
//    try {
//      httpOneEntry = client.guidCreate(masterGuid, "httpOneEntry" + RandomString.randomString(6));
//      httpTwoEntry = client.guidCreate(masterGuid, "httpTwoEntry" + RandomString.randomString(6));
//      System.out.println("Created: " + httpOneEntry);
//      System.out.println("Created: " + httpTwoEntry);
//    } catch (IOException | ClientException | NoSuchAlgorithmException e) {
//      failWithStackTrace("Exception in Http_CreateFields: " + e);
//    }
//  }
//  
//   @Test
//  public void test_902_Http_RemoveACL() {
//    try {
//      // remove default read acces for this test
//      client.aclRemove(AclAccessType.READ_WHITELIST, httpOneEntry,
//              GNSCommandProtocol.ENTIRE_RECORD, GNSCommandProtocol.ALL_GUIDS);
//    } catch (IOException | ClientException e) {
//      failWithStackTrace("Exception in Http_RemoveACL: " + e);
//    }
//  }
//
//  @Test
//  public void test_910_Http_UpdateFields() {
//    try {
//      client.fieldUpdate(httpOneEntry.getGuid(), "environment", "work", httpOneEntry);
//      client.fieldUpdate(httpOneEntry.getGuid(), "ssn", "000-00-0000", httpOneEntry);
//      client.fieldUpdate(httpOneEntry.getGuid(), "password", "666flapJack", httpOneEntry);
//      client.fieldUpdate(httpOneEntry.getGuid(), "address", "100 Hinkledinkle Drive", httpOneEntry);
//    } catch (IOException | ClientException | JSONException e) {
//      failWithStackTrace("Exception in Http_UpdateFields: " + e);
//    }
//  }

  private static final void failWithStackTrace(String message, Exception... e) {
    if (e != null && e.length > 0) {
      e[0].printStackTrace();
    }
    org.junit.Assert.fail(message);
  }
}
