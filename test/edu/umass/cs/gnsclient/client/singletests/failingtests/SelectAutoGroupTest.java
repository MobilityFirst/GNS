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
package edu.umass.cs.gnsclient.client.singletests.failingtests;

import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.util.GUIDUtilsHTTPClient;
import edu.umass.cs.gnscommon.utils.Base64;
import edu.umass.cs.gnsclient.client.util.BasicGuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.SHA1HashFunction;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.utils.RandomString;

import edu.umass.cs.gnsserver.utils.DefaultGNSTest;
import edu.umass.cs.utils.Utils;
import java.io.IOException;
import java.util.Arrays;

import java.util.HashSet;
import java.util.Set;
import org.hamcrest.Matchers;

import org.json.JSONArray;

import org.json.JSONException;
import org.junit.Assert;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * Comprehensive functionality test for the GNS.
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SelectAutoGroupTest extends DefaultGNSTest {

  private static GNSClientCommands clientCommands;
  private static GuidEntry masterGuid;
  private static final String groupTestFieldName = "_SelectAutoGroupTestQueryField_";
  private static GuidEntry groupOneGuid;
  private static GuidEntry groupTwoGuid;
  private static final String TEST_HIGH_VALUE = "25";
  private static final String TEST_LOW_VALUE = "10";
  private String queryOne = "~" + groupTestFieldName + " : {$gt: 20}";
  private String queryTwo = "~" + groupTestFieldName + " : 0";
  private static final Set<GuidEntry> createdGuids = new HashSet<>();

  /**
   *
   */
  public SelectAutoGroupTest() {
    if (clientCommands == null) {
      try {
        clientCommands = new GNSClientCommands();
        clientCommands.setForceCoordinatedReads(true);
      } catch (IOException e) {
        Utils.failWithStackTrace("Exception creating client: " + e);
      }
      try {
        masterGuid = GUIDUtilsHTTPClient.getGUIDKeys(globalAccountName);
      } catch (Exception e) {
        Utils.failWithStackTrace("Exception when we were not expecting it: " + e);
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
      JSONArray result = clientCommands.selectQuery(query);
      for (int i = 0; i < result.length(); i++) {
        BasicGuidEntry guidInfo = new BasicGuidEntry(clientCommands.lookupGuidRecord(result.getString(i)));
        GuidEntry guidEntry = GUIDUtilsHTTPClient.lookupGuidEntryFromDatabase(clientCommands, guidInfo.getEntityName());
        System.out.println("Removing from " + guidEntry.getEntityName());
        clientCommands.fieldRemove(guidEntry, groupTestFieldName);
      }
    } catch (ClientException | IOException | JSONException e) {
      Utils.failWithStackTrace("Trying to remove previous test's fields: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_552_QuerySetupGuids() {
    try {
      for (int cnt = 0; cnt < 5; cnt++) {
        GuidEntry testEntry = clientCommands.guidCreate(masterGuid, "queryTest-" + RandomString.randomString(12));
        createdGuids.add(testEntry);
        JSONArray array = new JSONArray(Arrays.asList(Integer.parseInt(TEST_HIGH_VALUE)));
        clientCommands.fieldReplaceOrCreateList(testEntry, groupTestFieldName, array);
      }
      for (int cnt = 0; cnt < 5; cnt++) {
        GuidEntry testEntry = clientCommands.guidCreate(masterGuid, "queryTest-" + RandomString.randomString(12));
        createdGuids.add(testEntry);
        JSONArray array = new JSONArray(Arrays.asList(Integer.parseInt(TEST_LOW_VALUE)));
        clientCommands.fieldReplaceOrCreateList(testEntry, groupTestFieldName, array);
      }
    } catch (ClientException | IOException | NumberFormatException e) {
      Utils.failWithStackTrace("Exception while trying to create the guids: " + e);
    }
    try {
      // the HRN is a hash of the query
      String groupOneGuidName = Base64.encodeToString(SHA1HashFunction.getInstance().hash(queryOne), false);
      groupOneGuid = clientCommands.guidCreate(masterGuid, groupOneGuidName);
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while trying to create the guids: " + e);
    }
    try {
      // the HRN is a hash of the query
      String groupTwoGuidName = Base64.encodeToString(SHA1HashFunction.getInstance().hash(queryTwo), false);
      groupTwoGuid = clientCommands.guidCreate(masterGuid, groupTwoGuidName);
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while trying to create the guids: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_553_QuerySetupGroup() {
    try {
      String query = "~" + groupTestFieldName + " : {$gt: 20}";
      JSONArray result = clientCommands.selectSetupGroupQuery(masterGuid, groupOneGuid.getPublicKeyString(), query, 0); // make the min refresh 0 seconds so the test will never Utils.failWithStackTrace
      System.out.println("*****SETUP guid named " + groupOneGuid.getEntityName() + ": ");
      for (int i = 0; i < result.length(); i++) {
        System.out.println(result.get(i).toString());
      }
      // best we can do should be at least 5, but possibly more objects in results
      Assert.assertThat(result.length(), Matchers.greaterThanOrEqualTo(5));
    } catch (ClientException | IOException | JSONException e) {
      Utils.failWithStackTrace("Exception executing selectSetupGroupQuery: " + e);
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
      JSONArray result = clientCommands.selectSetupGroupQuery(masterGuid, groupTwoGuid.getPublicKeyString(), query, 0); // make the min refresh 0 seconds so the test will never Utils.failWithStackTrace
      System.out.println("*****SETUP SECOND guid named " + groupTwoGuid.getEntityName() + ": (should be empty) ");
      for (int i = 0; i < result.length(); i++) {
        System.out.println(result.get(i).toString());
      }
      // should be nothing in this group now
      Assert.assertThat(result.length(), Matchers.equalTo(0));
    } catch (ClientException | IOException | JSONException e) {
      Utils.failWithStackTrace("Exception executing second selectSetupGroupQuery: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_555_QueryLookupGroup() {
    try {
      JSONArray result = clientCommands.selectLookupGroupQuery(groupOneGuid.getGuid());
      checkSelectTheReturnValues(result);
    } catch (Exception e) {
      Utils.failWithStackTrace("Exception executing selectLookupGroupQuery: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_556_QueryLookupGroupAgain() {
    try {
      JSONArray result = clientCommands.selectLookupGroupQuery(groupOneGuid.getGuid());
      checkSelectTheReturnValues(result);
    } catch (Exception e) {
      Utils.failWithStackTrace("Exception executing selectLookupGroupQuery: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_557_LookupGroupAgain2() {
    try {
      JSONArray result = clientCommands.selectLookupGroupQuery(groupOneGuid.getGuid());
      checkSelectTheReturnValues(result);
    } catch (Exception e) {
      Utils.failWithStackTrace("Exception executing selectLookupGroupQuery: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_558_QueryLookupGroupAgain3() {
    try {
      JSONArray result = clientCommands.selectLookupGroupQuery(groupOneGuid.getGuid());
      checkSelectTheReturnValues(result);
    } catch (Exception e) {
      Utils.failWithStackTrace("Exception executing selectLookupGroupQuery: " + e);
    }
  }

  /**
   *
   */
  @Test
  // Change all the testQuery fields except 1 to be equal to zero
  public void test_559_QueryAlterGroup() {
    try {
      JSONArray result = clientCommands.selectLookupGroupQuery(groupOneGuid.getGuid());
      // change ALL BUT ONE to be ZERO
      for (int i = 0; i < result.length() - 1; i++) {
        BasicGuidEntry guidInfo = new BasicGuidEntry(clientCommands.lookupGuidRecord(result.getString(i)));
        GuidEntry entry = GUIDUtilsHTTPClient.lookupGuidEntryFromDatabase(clientCommands, guidInfo.getEntityName());
        JSONArray array = new JSONArray(Arrays.asList(0));
        clientCommands.fieldReplaceOrCreateList(entry, groupTestFieldName, array);
      }
    } catch (ClientException | IOException | JSONException e) {
      Utils.failWithStackTrace("Exception while trying to alter the fields: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_560_QueryLookupGroupAfterAlterations() {
    try {
      JSONArray result = clientCommands.selectLookupGroupQuery(groupOneGuid.getGuid());
      // should only be one
      Assert.assertThat(result.length(), Matchers.equalTo(1));
      // look up the individual values
      for (int i = 0; i < result.length(); i++) {
        BasicGuidEntry guidInfo = new BasicGuidEntry(clientCommands.lookupGuidRecord(result.getString(i)));
        GuidEntry entry = GUIDUtilsHTTPClient.lookupGuidEntryFromDatabase(clientCommands, guidInfo.getEntityName());
        String value = clientCommands.fieldReadArrayFirstElement(entry, groupTestFieldName);
        Assert.assertEquals(TEST_HIGH_VALUE, value);
      }
    } catch (ClientException | IOException | JSONException e) {
      Utils.failWithStackTrace("Exception executing selectLookupGroupQuery: " + e);
    }
  }

  /**
   *
   */
  @Test
  // Check to see if the second group has members now... it should.
  public void test_561_QueryLookupSecondGroup() {
    try {
      JSONArray result = clientCommands.selectLookupGroupQuery(groupTwoGuid.getGuid());
      // should be 4 now
      Assert.assertThat(result.length(), Matchers.equalTo(4));
      // look up the individual values
      for (int i = 0; i < result.length(); i++) {
        BasicGuidEntry guidInfo = new BasicGuidEntry(clientCommands.lookupGuidRecord(result.getString(i)));
        GuidEntry entry = GUIDUtilsHTTPClient.lookupGuidEntryFromDatabase(clientCommands, guidInfo.getEntityName());
        String value = clientCommands.fieldReadArrayFirstElement(entry, groupTestFieldName);
        Assert.assertEquals("0", value);
      }
    } catch (ClientException | IOException | JSONException e) {
      Utils.failWithStackTrace("Exception executing selectLookupGroupQuery: " + e);
    }
  }
  
  /**
   *
   */
  @Test
  public void test_562_QueryLookupSelectCleanup() {
    try {
      clientCommands.guidRemove(masterGuid, groupOneGuid.getGuid());
      clientCommands.guidRemove(masterGuid, groupTwoGuid.getGuid());
      for (GuidEntry guid : createdGuids) {
        clientCommands.guidRemove(masterGuid, guid.getGuid());
      }
      createdGuids.clear();
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception during cleanup: " + e);
    }
  }

  private void checkSelectTheReturnValues(JSONArray result) throws Exception {
    // should be 5
    Assert.assertThat(result.length(), Matchers.equalTo(5));
    // look up the individual values
    for (int i = 0; i < result.length(); i++) {
      BasicGuidEntry guidInfo = new BasicGuidEntry(clientCommands.lookupGuidRecord(result.getString(i)));
      GuidEntry entry = GUIDUtilsHTTPClient.lookupGuidEntryFromDatabase(clientCommands, guidInfo.getEntityName());
      String value = clientCommands.fieldReadArrayFirstElement(entry, groupTestFieldName);
      Assert.assertEquals(TEST_HIGH_VALUE, value);
    }
  }

}
