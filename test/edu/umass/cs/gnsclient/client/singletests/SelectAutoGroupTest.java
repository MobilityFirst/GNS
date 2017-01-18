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
package edu.umass.cs.gnsclient.client.singletests;


import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnscommon.utils.Base64;
import edu.umass.cs.gnsclient.client.util.BasicGuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnsclient.client.util.SHA1HashFunction;
import edu.umass.cs.gnscommon.utils.RandomString;

import java.io.IOException;
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

  private static final String ACCOUNT_ALIAS = "admin@gns.name"; // REPLACE THIS WITH YOUR ACCOUNT ALIAS
  private static final String PASSWORD = "password";
  private static GNSClientCommands client;
  private static GuidEntry masterGuid;
  private static final String groupTestFieldName = "_SelectAutoGroupTestQueryField_";
  private static GuidEntry groupOneGuid;
  private static GuidEntry groupTwoGuid;
  private static final String TEST_HIGH_VALUE = "25";
  private static final String TEST_LOW_VALUE = "10";
  private String queryOne = "~" + groupTestFieldName + " : {$gt: 20}";
  private String queryTwo = "~" + groupTestFieldName + " : 0";
  
  /**
   *
   */
  public SelectAutoGroupTest() {
    if (client == null) {
       try {
        client = new GNSClientCommands();
        client.setForceCoordinatedReads(true);
      } catch (IOException e) {
        fail("Exception creating client: " + e);
      }
      try {
        masterGuid = GuidUtils.lookupOrCreateAccountGuid(client, ACCOUNT_ALIAS, PASSWORD, true);
      } catch (Exception e) {
        fail("Exception when we were not expecting it: " + e);
      }
    }
  }

  /**
   *
   */
  @Test
  public void test_551_QueryRemovePreviousTestFields() {
    // find all the guids that have our field and remove it from them
    try {
      String query = "~" + groupTestFieldName + " : {$exists: true}";
      JSONArray result = client.selectQuery(query);
      for (int i = 0; i < result.length(); i++) {
        BasicGuidEntry guidInfo = new BasicGuidEntry(client.lookupGuidRecord(result.getString(i)));
        GuidEntry guidEntry = GuidUtils.lookupGuidEntryFromDatabase(client, guidInfo.getEntityName());
        System.out.println("Removing from " + guidEntry.getEntityName());
        client.fieldRemove(guidEntry, groupTestFieldName);
      }
    } catch (Exception e) {
      fail("Trying to remove previous test's fields: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_552_QuerySetupGuids() {
    try {
      for (int cnt = 0; cnt < 5; cnt++) {
        GuidEntry testEntry = client.guidCreate(masterGuid, "queryTest-" + RandomString.randomString(6));
        JSONArray array = new JSONArray(Arrays.asList(Integer.parseInt(TEST_HIGH_VALUE)));
        client.fieldReplaceOrCreateList(testEntry, groupTestFieldName, array);
      }
      for (int cnt = 0; cnt < 5; cnt++) {
        GuidEntry testEntry = client.guidCreate(masterGuid, "queryTest-" + RandomString.randomString(6));
        JSONArray array = new JSONArray(Arrays.asList(Integer.parseInt(TEST_LOW_VALUE)));
        client.fieldReplaceOrCreateList(testEntry, groupTestFieldName, array);
      }
    } catch (Exception e) {
      fail("Exception while trying to create the guids: " + e);
    }
    try {
      // the HRN is a hash of the query
      String groupOneGuidName = Base64.encodeToString(SHA1HashFunction.getInstance().hash(queryOne), false);
      groupOneGuid = GuidUtils.lookupOrCreateGuidEntry(groupOneGuidName, GNSClientCommands.getGNSProvider());
      //groupGuid = client.guidCreate(masterGuid, groupGuidName + RandomString.randomString(6));
    } catch (Exception e) {
      e.printStackTrace();
      fail("Exception while trying to create the guids: " + e);
    }
    try {
      // the HRN is a hash of the query
      String groupTwoGuidName = Base64.encodeToString(SHA1HashFunction.getInstance().hash(queryTwo), false);
      groupTwoGuid = GuidUtils.lookupOrCreateGuidEntry(groupTwoGuidName, GNSClientCommands.getGNSProvider());
      //groupTwoGuid = client.guidCreate(masterGuid, groupTwoGuidName + RandomString.randomString(6));
    } catch (Exception e) {
      e.printStackTrace();
      fail("Exception while trying to create the guids: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_553_QuerySetupGroup() {
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

  /**
   *
   */
  @Test
  public void test_554_QuerySetupSecondGroup() {
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

  /**
   *
   */
  @Test
  public void test_555_QueryLookupGroup() {
    try {
      JSONArray result = client.selectLookupGroupQuery(groupOneGuid.getGuid());
      checkSelectTheReturnValues(result);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Exception executing selectLookupGroupQuery: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_556_QueryLookupGroupAgain() {
    try {
      JSONArray result = client.selectLookupGroupQuery(groupOneGuid.getGuid());
      checkSelectTheReturnValues(result);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Exception executing selectLookupGroupQuery: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_557_LookupGroupAgain2() {
    try {
      JSONArray result = client.selectLookupGroupQuery(groupOneGuid.getGuid());
      checkSelectTheReturnValues(result);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Exception executing selectLookupGroupQuery: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_558_QueryLookupGroupAgain3() {
    try {
      JSONArray result = client.selectLookupGroupQuery(groupOneGuid.getGuid());
      checkSelectTheReturnValues(result);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Exception executing selectLookupGroupQuery: " + e);
    }
  }

  /**
   *
   */
  @Test
  // Change all the testQuery fields except 1 to be equal to zero
  public void test_559_QueryAlterGroup() {
    try {
      JSONArray result = client.selectLookupGroupQuery(groupOneGuid.getGuid());
      // change ALL BUT ONE to be ZERO
      for (int i = 0; i < result.length() - 1; i++) {
        BasicGuidEntry guidInfo = new BasicGuidEntry(client.lookupGuidRecord(result.getString(i)));
        GuidEntry entry = GuidUtils.lookupGuidEntryFromDatabase(client, guidInfo.getEntityName());
        JSONArray array = new JSONArray(Arrays.asList(0));
        client.fieldReplaceOrCreateList(entry, groupTestFieldName, array);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail("Exception while trying to alter the fields: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_560_QueryLookupGroupAfterAlterations() {
    try {
      JSONArray result = client.selectLookupGroupQuery(groupOneGuid.getGuid());
      // should only be one
      assertThat(result.length(), equalTo(1));
      // look up the individual values
      for (int i = 0; i < result.length(); i++) {
        BasicGuidEntry guidInfo = new BasicGuidEntry(client.lookupGuidRecord(result.getString(i)));
        GuidEntry entry = GuidUtils.lookupGuidEntryFromDatabase(client, guidInfo.getEntityName());
        String value = client.fieldReadArrayFirstElement(entry, groupTestFieldName);
        assertEquals(TEST_HIGH_VALUE, value);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail("Exception executing selectLookupGroupQuery: " + e);
    }
  }

  /**
   *
   */
  @Test
  // Check to see if the second group has members now... it should.
  public void test_561_QueryLookupSecondGroup() {
    try {
      JSONArray result = client.selectLookupGroupQuery(groupTwoGuid.getGuid());
      // should be 4 now
      assertThat(result.length(), equalTo(4));
      // look up the individual values
      for (int i = 0; i < result.length(); i++) {
        BasicGuidEntry guidInfo = new BasicGuidEntry(client.lookupGuidRecord(result.getString(i)));
        GuidEntry entry = GuidUtils.lookupGuidEntryFromDatabase(client, guidInfo.getEntityName());
        String value = client.fieldReadArrayFirstElement(entry, groupTestFieldName);
        assertEquals("0", value);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail("Exception executing selectLookupGroupQuery: " + e);
    }
  }
  
  

  private void checkSelectTheReturnValues(JSONArray result) throws Exception {
    // should be 5
    assertThat(result.length(), equalTo(5));
    // look up the individual values
    for (int i = 0; i < result.length(); i++) {
      BasicGuidEntry guidInfo = new BasicGuidEntry(client.lookupGuidRecord(result.getString(i)));
      GuidEntry entry = GuidUtils.lookupGuidEntryFromDatabase(client, guidInfo.getEntityName());
      String value = client.fieldReadArrayFirstElement(entry, groupTestFieldName);
      assertEquals(TEST_HIGH_VALUE, value);
    }
  }

}
