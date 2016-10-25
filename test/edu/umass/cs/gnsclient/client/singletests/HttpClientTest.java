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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import org.junit.Test;

/**
 *
 * @author westy
 */
public class HttpClientTest {
  
   private static GuidEntry masterGuid;
  private static final String ACCOUNT_ALIAS = "test@gns.name"; // REPLACE THIS WITH YOUR ACCOUNT ALIAS
  private static final String PASSWORD = "password";

  private HttpClient client;
  
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
    } catch (Exception e) {
      fail("Exception in LookupGuid: " + e);
    }
  }

}
