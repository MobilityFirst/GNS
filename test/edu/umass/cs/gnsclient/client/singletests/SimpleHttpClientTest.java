/*
 * Copyright (C) 2016
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gnsclient.client.singletests;

import edu.umass.cs.gnsclient.client.http.HttpClient;
import edu.umass.cs.gnsclient.client.util.GUIDUtilsHTTPClient;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.utils.RandomString;
import edu.umass.cs.gnsserver.utils.DefaultGNSTest;
import edu.umass.cs.utils.Utils;
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
public class SimpleHttpClientTest extends DefaultGNSTest {

  private HttpClient httpClient;

  private static GuidEntry masterGuid;
  private static GuidEntry httpOneEntry;
  private static GuidEntry httpTwoEntry;


  /**
   *
   */
  public SimpleHttpClientTest() {
    if (httpClient == null) {
      httpClient = new HttpClient("127.0.0.1", 24703);
    }
  }

  /**
   *
   */
  @Test
  public void test_900_Http_CreateAccountGuid() {
    try {
      masterGuid = GUIDUtilsHTTPClient.getGUIDKeys(globalAccountName);

    } catch (Exception e) {
      Utils.failWithStackTrace("Exception while creating master guid: ", e);
    }
  }

  /**
   *
   */
  @Test
  public void test_901_Http_LookupGuid() {
    try {
      Assert.assertEquals(masterGuid.getGuid(), httpClient.lookupGuid(globalAccountName));
    } catch (IOException | ClientException e) {
      Utils.failWithStackTrace("Exception in LookupGuid: ", e);
    }
  }

  /**
   *
   */
  @Test
  public void test_902_Http_CreateGuids() {
    try {
      httpOneEntry = httpClient.guidCreate(masterGuid, "httpOneEntry" + RandomString.randomString(12));
      httpTwoEntry = httpClient.guidCreate(masterGuid, "httpTwoEntry" + RandomString.randomString(12));
      System.out.println("Created: " + httpOneEntry);
      System.out.println("Created: " + httpTwoEntry);
    } catch (IOException | ClientException | NoSuchAlgorithmException e) {
      Utils.failWithStackTrace("Exception in Http_CreateFields: ", e);
    }
  }

  /**
   *
   */
  @Test
  public void test_904_Http_UpdateFields() {
    try {
      httpClient.fieldUpdate(httpOneEntry.getGuid(), "environment", "work", httpOneEntry);
    } catch (IOException | ClientException | JSONException e) {
      Utils.failWithStackTrace("Exception in Http_UpdateFields: ", e);
    }
  }
  
   /*
   *
   */
  @Test
  public void test_999_Cleanup() {
    try {
      httpClient.guidRemove(masterGuid, httpOneEntry.getGuid());
      httpClient.guidRemove(masterGuid, httpTwoEntry.getGuid());
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while removing guids: " + e);
    }
  }
}
