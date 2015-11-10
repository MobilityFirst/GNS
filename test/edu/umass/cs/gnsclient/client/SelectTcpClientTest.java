/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gnsclient.client;

import edu.umass.cs.gnsclient.client.UniversalTcpClientExtended;
import edu.umass.cs.gnsclient.client.GuidEntry;
import edu.umass.cs.gnsclient.client.util.Utils;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnsclient.client.util.ServerSelectDialog;
import java.net.InetSocketAddress;
import java.util.Arrays;
import static org.hamcrest.Matchers.*;
import org.json.JSONArray;
import static org.junit.Assert.*;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * Comprehensive functionality test for the GNS.
 * 
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SelectTcpClientTest {

  private static final String ACCOUNT_ALIAS = "david@westy.org"; // REPLACE THIS WITH YOUR ACCOUNT ALIAS
  private static final String PASSWORD = "password";
  private static UniversalTcpClientExtended client;
  /**
   * The address of the GNS server we will contact
   */
  private static InetSocketAddress address = null;
  private static GuidEntry masterGuid;

  public SelectTcpClientTest() {
    if (address == null) {
      address = ServerSelectDialog.selectServer();
      client = new UniversalTcpClientExtended(address.getHostName(), address.getPort(), true);
      try {
        masterGuid = GuidUtils.lookupOrCreateAccountGuid(client, ACCOUNT_ALIAS, PASSWORD, true);
      } catch (Exception e) {
        fail("Exception when we were not expecting it: " + e);
      }
    }
  }

  @Test
  public void test_01_testQuerySelect() {
    String fieldName = "testQuery";
    try {
      for (int cnt = 0; cnt < 5; cnt++) {
        GuidEntry testEntry = GuidUtils.registerGuidWithTestTag(client, masterGuid, "queryTest-" + Utils.randomString(6));
       JSONArray array = new JSONArray(Arrays.asList(25));
       client.fieldReplaceOrCreateList(testEntry, fieldName, array);
      }
    } catch (Exception e) {
      fail("Exception while trying to create the guids: " + e);
    }

    try {
      String query = "~" + fieldName + " : ($gt: 0)";
      JSONArray result = client.selectQuery(query);
      for (int i = 0; i < result.length(); i++) {
        System.out.println(result.get(i).toString());
      }
      // best we can do should be at least 5, but possibly more objects in results
      assertThat(result.length(), greaterThanOrEqualTo(5));
    } catch (Exception e) {
      fail("Exception executing selectNear: " + e);
    }
  }

}
