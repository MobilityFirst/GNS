/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *  Initial developer(s): Westy
 *
 */
package edu.umass.cs.gnsclient.client;

import edu.umass.cs.gnsclient.client.UniversalTcpClientExtended;
import edu.umass.cs.gnsclient.client.BasicGuidEntry;
import edu.umass.cs.gnsclient.client.GuidEntry;
import edu.umass.cs.gnscommon.utils.Base64;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnsclient.client.util.SHA1HashFunction;
import edu.umass.cs.gnsclient.client.util.ServerSelectDialog;
import edu.umass.cs.gnscommon.utils.RandomString;
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
public class SelectAutoGroupTest {

  private static final String ACCOUNT_ALIAS = "westy@cs.umass.edu"; // REPLACE THIS WITH YOUR ACCOUNT ALIAS
  private static final String PASSWORD = "password";
  private static UniversalTcpClientExtended client;
  /**
   * The address of the GNS server we will contact
   */
  private static InetSocketAddress address = null;
  private static GuidEntry masterGuid;
  private static final String groupTestFieldName = "_SelectAutoGroupTestQueryField_";
  private static GuidEntry groupOneGuid;
  private static GuidEntry groupTwoGuid;
  private String queryOne = "~" + groupTestFieldName + " : {$gt: 20}";
  private String queryTwo = "~" + groupTestFieldName + " : 0";
  

  public SelectAutoGroupTest() {
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
  public void test_01_QueryRemovePreviousTestFields() {
    // find all the guids that have our field and remove it from them
    try {
      String query = "~" + groupTestFieldName + " : {$exists: true}";
      JSONArray result = client.selectQuery(query);
      for (int i = 0; i < result.length(); i++) {
        BasicGuidEntry guidInfo = new BasicGuidEntry(client.lookupGuidRecord(result.getString(i)));
        GuidEntry guidEntry = GuidUtils.lookupGuidEntryFromPreferences(client, guidInfo.getEntityName());
        System.out.println("Removing from " + guidEntry.getEntityName());
        client.fieldRemove(guidEntry, groupTestFieldName);
      }
    } catch (Exception e) {
      fail("Trying to remove previous test's fields: " + e);
    }
  }

  @Test
  public void test_02_QuerySetupGuids() {
    try {
      for (int cnt = 0; cnt < 5; cnt++) {
        GuidEntry testEntry = GuidUtils.registerGuidWithTestTag(client, masterGuid, "queryTest-" + RandomString.randomString(6));
        JSONArray array = new JSONArray(Arrays.asList(25));
        client.fieldReplaceOrCreateList(testEntry, groupTestFieldName, array);
      }
      for (int cnt = 0; cnt < 5; cnt++) {
        GuidEntry testEntry = GuidUtils.registerGuidWithTestTag(client, masterGuid, "queryTest-" + RandomString.randomString(6));
        JSONArray array = new JSONArray(Arrays.asList(10));
        client.fieldReplaceOrCreateList(testEntry, groupTestFieldName, array);
      }
    } catch (Exception e) {
      fail("Exception while trying to create the guids: " + e);
    }
    try {
      // the HRN is a hash of the query
      String groupOneGuidName = Base64.encodeToString(SHA1HashFunction.getInstance().hash(queryOne.getBytes()), false);
      groupOneGuid = GuidUtils.lookupOrCreateGuidEntry(groupOneGuidName, client.getGnsRemoteHost(), client.getGnsRemotePort());
      //groupGuid = GuidUtils.registerGuidWithTestTag(client, masterGuid, groupGuidName + RandomString.randomString(6));
    } catch (Exception e) {
      e.printStackTrace();
      fail("Exception while trying to create the guids: " + e);
    }
    try {
      // the HRN is a hash of the query
      String groupTwoGuidName = Base64.encodeToString(SHA1HashFunction.getInstance().hash(queryTwo.getBytes()), false);
      groupTwoGuid = GuidUtils.lookupOrCreateGuidEntry(groupTwoGuidName, client.getGnsRemoteHost(), client.getGnsRemotePort());
      //groupTwoGuid = GuidUtils.registerGuidWithTestTag(client, masterGuid, groupTwoGuidName + RandomString.randomString(6));
    } catch (Exception e) {
      e.printStackTrace();
      fail("Exception while trying to create the guids: " + e);
    }
  }

  @Test
  public void test_03_QuerySetupGroup() {
    try {
      String query = "~" + groupTestFieldName + " : {$gt: 20}";
      JSONArray result = client.selectSetupGroupQuery(masterGuid, groupOneGuid.getPublicKeyString(), query, 0); // make the min refresh 0 seconds so the test will never fail
      System.out.println("*****SETUP guid named " + groupOneGuid.getEntityName() + ": ");
      for (int i = 0; i < result.length(); i++) {
        System.out.println(result.get(i).toString());
      }
      // best we can do should be at least 5, but possibly more objects in results
      assertThat(result.length(), greaterThanOrEqualTo(5));
    } catch (Exception e) {
      e.printStackTrace();
      fail("Exception executing selectSetupGroupQuery: " + e);
    }
  }

  // make a second group that is empty
  @Test
  public void test_04_QuerySetupSecondGroup() {
    try {
      String query = "~" + groupTestFieldName + " : 0";
      JSONArray result = client.selectSetupGroupQuery(masterGuid, groupTwoGuid.getPublicKeyString(), query, 0); // make the min refresh 0 seconds so the test will never fail
      System.out.println("*****SETUP SECOND guid named " + groupTwoGuid.getEntityName() + ": (should be empty) ");
      for (int i = 0; i < result.length(); i++) {
        System.out.println(result.get(i).toString());
      }
      // should be nothing in this group now
      assertThat(result.length(), equalTo(0));
    } catch (Exception e) {
      e.printStackTrace();
      fail("Exception executing second selectSetupGroupQuery: " + e);
    }
  }

  @Test
  public void test_05_QueryLookupGroup() {
    try {
      JSONArray result = client.selectLookupGroupQuery(groupOneGuid.getGuid());
      checkTheReturnValues(result);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Exception executing selectLookupGroupQuery: " + e);
    }
  }
//
//  @Test
//  @Order(6)
//  public void testQueryLookupGroupAgain() {
//    try {
//      JSONArray result = client.selectLookupGroupQuery(groupGuid.getGuid());
//      checkTheReturnValues(result);
//    } catch (Exception e) {
//      e.printStackTrace();
//      fail("Exception executing selectLookupGroupQuery: " + e);
//    }
//  }
//
//  @Test
//  @Order(7)
//  public void testQueryLookupGroupAgain2() {
//    try {
//      JSONArray result = client.selectLookupGroupQuery(groupGuid.getGuid());
//      checkTheReturnValues(result);
//    } catch (Exception e) {
//      e.printStackTrace();
//      fail("Exception executing selectLookupGroupQuery: " + e);
//    }
//  }
//
//  @Test
//  @Order(8)
//  public void testQueryLookupGroupAgain3() {
//    try {
//      JSONArray result = client.selectLookupGroupQuery(groupGuid.getGuid());
//      checkTheReturnValues(result);
//    } catch (Exception e) {
//      e.printStackTrace();
//      fail("Exception executing selectLookupGroupQuery: " + e);
//    }
//  }
//
//  @Test
//  @Order(9)
//  // Change all the testQuery fields except 1 to be equal to zero
//  public void testQueryAlterGroup() {
//    try {
//      JSONArray result = client.selectLookupGroupQuery(groupGuid.getGuid());
//      // change ALL BUT ONE to be ZERO
//      for (int i = 0; i < result.length() - 1; i++) {
//        BasicGuidEntry guidInfo = new BasicGuidEntry(client.lookupGuidRecord(result.getString(i)));
//        GuidEntry entry = RandomString.lookupGuidEntryFromPreferences(client, guidInfo.getEntityName());
//        JSONArray array = new JSONArray(Arrays.asList(0));
//        client.fieldReplaceOrCreateList(entry, fieldName, array);
//      }
//    } catch (Exception e) {
//      e.printStackTrace();
//      fail("Exception while trying to alter the fields: " + e);
//    }
//  }
//
//  @Test
//  @Order(10)
//  public void testQueryLookupGroupAfterAlterations() {
//    try {
//      JSONArray result = client.selectLookupGroupQuery(groupGuid.getGuid());
//      // should only be one
//      assertThat(result.length(), equalTo(1));
//      // look up the individual values
//      for (int i = 0; i < result.length(); i++) {
//        BasicGuidEntry guidInfo = new BasicGuidEntry(client.lookupGuidRecord(result.getString(i)));
//        GuidEntry entry = RandomString.lookupGuidEntryFromPreferences(client, guidInfo.getEntityName());
//        String value = client.fieldReadArrayFirstElement(entry, fieldName);
//        assertEquals("25", value);
//      }
//    } catch (Exception e) {
//      e.printStackTrace();
//      fail("Exception executing selectLookupGroupQuery: " + e);
//    }
//  }
//
//  @Test
//  @Order(11)
//  // Check to see if the second group has members now... it should.
//  public void testQueryLookupSecondGroup() {
//    try {
//      JSONArray result = client.selectLookupGroupQuery(groupTwoGuid.getGuid());
//      // should be 4 now
//      assertThat(result.length(), equalTo(4));
//      // look up the individual values
//      for (int i = 0; i < result.length(); i++) {
//        BasicGuidEntry guidInfo = new BasicGuidEntry(client.lookupGuidRecord(result.getString(i)));
//        GuidEntry entry = RandomString.lookupGuidEntryFromPreferences(client, guidInfo.getEntityName());
//        String value = client.fieldReadArrayFirstElement(entry, fieldName);
//        assertEquals("0", value);
//      }
//    } catch (Exception e) {
//      e.printStackTrace();
//      fail("Exception executing selectLookupGroupQuery: " + e);
//    }
//  }
//  
//  
  private void checkTheReturnValues(JSONArray result) throws Exception {
    // should be 5
    assertThat(result.length(), equalTo(5));
    // look up the individual values
    for (int i = 0; i < result.length(); i++) {
      BasicGuidEntry guidInfo = new BasicGuidEntry(client.lookupGuidRecord(result.getString(i)));
      GuidEntry entry = GuidUtils.lookupGuidEntryFromPreferences(client, guidInfo.getEntityName());
      String value = client.fieldReadArrayFirstElement(entry, groupTestFieldName);
      assertEquals("25", value);
    }
  }

}
