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
import edu.umass.cs.gnsclient.client.GNSCommand;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnsclient.client.util.JSONUtils;
import edu.umass.cs.gnsclient.jsonassert.JSONAssert;
import edu.umass.cs.gnsclient.jsonassert.JSONCompareMode;
import edu.umass.cs.gnscommon.AclAccessType;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.utils.RandomString;

import edu.umass.cs.gnscommon.utils.StringUtil;
import edu.umass.cs.gnsserver.utils.DefaultGNSTest;
import edu.umass.cs.utils.Utils;
import java.awt.geom.Point2D;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import java.util.Set;
import org.hamcrest.Matchers;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import org.junit.Assert;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * Functionality test for core elements in the client using the UniversalGnsClientFull.
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SelectTest extends DefaultGNSTest {

  private static final int WAIT_SETTLE = 200;

  //private static GNSClientCommands clientCommands = null;
  private static GuidEntry masterGuid;
  private static GuidEntry westyEntry;
  private static GuidEntry samEntry;
  private static final Set<GuidEntry> CREATED_GUIDS = new HashSet<>();

  /**
   *
   */
  public SelectTest() {
//    if (clientCommands == null) {
//      try {
//        clientCommands = new GNSClientCommands();
//        clientCommands.setForceCoordinatedReads(true);
//      } catch (IOException e) {
//        Utils.failWithStackTrace("Exception creating client: " + e);
//      }
    try {
      masterGuid = GuidUtils.getGUIDKeys(globalAccountName);
    } catch (Exception e) {
      Utils.failWithStackTrace("Exception while creating account guid: " + e);
    }
    //}
  }

  /**
   * Create the guids
   */
  @Test
  public void test_010_CreateGuids() {
    try {
      String westyName = "westy" + RandomString.randomString(12);
      String samName = "sam" + RandomString.randomString(12);
      client.execute(GNSCommand.createGUID(masterGuid, westyName));
      westyEntry = GuidUtils.getGUIDKeys(westyName);
      client.execute(GNSCommand.createGUID(masterGuid, samName));
      samEntry = GuidUtils.getGUIDKeys(samName);
//      westyEntry = clientCommands.guidCreate(masterGuid, "westy" + RandomString.randomString(12));
//      samEntry = clientCommands.guidCreate(masterGuid, "sam" + RandomString.randomString(12));
      System.out.println("Created: " + westyEntry);
      System.out.println("Created: " + samEntry);
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception registering guids: " + e);
    }
  }

//  /**
//   * Create the fields
//   */
//  @Test
//  public void test_020_Cats() {
//    try {
//      client.execute(GNSCommand.fieldCreateOneElementList(westyEntry.getGuid(), "cats", "fred", westyEntry));
//      //clientCommands.fieldCreateOneElementList(westyEntry.getGuid(), "cats", "whacky", westyEntry);
//
//      Assert.assertEquals("fred",
//              client.execute(GNSCommand.fieldReadArrayFirstElement(westyEntry.getGuid(),
//                      "cats", westyEntry)).getResultString());
////      Assert.assertEquals("whacky",
////              clientCommands.fieldReadArrayFirstElement(westyEntry.getGuid(), "cats", westyEntry));
//    } catch (IOException | ClientException e) {
//      Utils.failWithStackTrace("Exception when we were not expecting testing DB: " + e);
//    }
//  }
//
//  /**
//   * Check the basic field select command
//   */
//  @Test
//  public void test_030_BasicSelect() {
//    try {
//      waitSettle(WAIT_SETTLE);
//      JSONArray result = client.execute(GNSCommand.select(masterGuid, "cats", "fred")).getResultJSONArray();
//      //JSONArray result = clientCommands.select(masterGuid, "cats", "fred");
//      // best we can do since there will be one, but possibly more objects in results
//      Assert.assertThat(result.length(), Matchers.greaterThanOrEqualTo(1));
//    } catch (ClientException | IOException e) {
//      Utils.failWithStackTrace("Exception when we were not expecting it: " + e);
//    }
//  }
//
//  /**
//   * Check a near and within commands
//   */
//  @Test
//  public void test_040_GeoSpatialSelect() {
//    try {
//      for (int cnt = 0; cnt < 5; cnt++) {
//        GuidEntry testEntry = clientCommands.guidCreate(masterGuid, "geoTest-" + RandomString.randomString(12));
//        CREATED_GUIDS.add(testEntry); // save them so we can delete them later
//        clientCommands.setLocation(testEntry, 0.0, 0.0);
//        waitSettle(WAIT_SETTLE);
//      }
//    } catch (ClientException | IOException e) {
//      Utils.failWithStackTrace("Exception when we were not expecting it: " + e);
//    }
//
//    try {
//
//      JSONArray loc = new JSONArray();
//      loc.put(1.0);
//      loc.put(1.0);
//      JSONArray result = clientCommands.selectNear(masterGuid, GNSProtocol.LOCATION_FIELD_NAME.toString(), loc, 2000000.0);
//      // best we can do should be at least 5, but possibly more objects in results
//      Assert.assertThat(result.length(), Matchers.greaterThanOrEqualTo(5));
//    } catch (JSONException | ClientException | IOException e) {
//      Utils.failWithStackTrace("Exception executing selectNear: " + e);
//    }
//
//    try {
//
//      JSONArray rect = new JSONArray();
//      JSONArray upperLeft = new JSONArray();
//      upperLeft.put(1.0);
//      upperLeft.put(1.0);
//      JSONArray lowerRight = new JSONArray();
//      lowerRight.put(-1.0);
//      lowerRight.put(-1.0);
//      rect.put(upperLeft);
//      rect.put(lowerRight);
//      JSONArray result = clientCommands.selectWithin(masterGuid, GNSProtocol.LOCATION_FIELD_NAME.toString(), rect);
//      // best we can do should be at least 5, but possibly more objects in results
//      Assert.assertThat(result.length(), Matchers.greaterThanOrEqualTo(5));
//    } catch (JSONException | ClientException | IOException e) {
//      Utils.failWithStackTrace("Exception executing selectWithin: " + e);
//    }
//  }
//

  /**
   * Check a query select with a reader
   */
  @Test
  public void test_050_QuerySelectwithReader() {
    String fieldName = "testQuery";
    try {
      for (int cnt = 0; cnt < 5; cnt++) {
        String queryTestName = "queryTest-" + RandomString.randomString(12);
        client.execute(GNSCommand.createGUID(masterGuid, queryTestName));
        GuidEntry testEntry = GuidUtils.getGUIDKeys(queryTestName);

        // Remove default all fields / all guids ACL;
        client.execute(GNSCommand.aclRemove(AclAccessType.READ_WHITELIST, testEntry,
                GNSProtocol.ENTIRE_RECORD.toString(), GNSProtocol.ALL_GUIDS.toString()));
        CREATED_GUIDS.add(testEntry); // save them so we can delete them later
        JSONArray array = new JSONArray(Arrays.asList(25));
        client.execute(GNSCommand.fieldReplaceOrCreateList(testEntry.getGuid(), fieldName, array, testEntry));
      }
      waitSettle(WAIT_SETTLE);
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while trying to create the guids: " + e);
    }

    try {
      String query = "~" + fieldName + " : ($gt: 0)";
      JSONArray result = client.execute(GNSCommand.selectQuery(masterGuid, query)).getResultJSONArray();
      for (int i = 0; i < result.length(); i++) {
        System.out.println(result.get(i).toString());
      }
      // best we can do should be at least 5, but possibly more objects in results
      Assert.assertThat(result.length(), Matchers.greaterThanOrEqualTo(5));
    } catch (ClientException | IOException | JSONException e) {
      Utils.failWithStackTrace("Exception executing selectQuery: " + e);
    }
  }

  /**
   * Check a query select with world readable fields
   */
  @Test
  public void test_053_QuerySelectWorldReadable() {
    String fieldName = "testQueryWorldReadable";
    try {
      for (int cnt = 0; cnt < 5; cnt++) {
        String queryTestName = "queryTest-" + RandomString.randomString(12);
        client.execute(GNSCommand.createGUID(masterGuid, queryTestName));
        GuidEntry testEntry = GuidUtils.getGUIDKeys(queryTestName);
        CREATED_GUIDS.add(testEntry); // save them so we can delete them later
        JSONArray array = new JSONArray(Arrays.asList(25));
        client.execute(GNSCommand.fieldReplaceOrCreateList(testEntry.getGuid(), fieldName, array, testEntry));
      }
      waitSettle(WAIT_SETTLE);
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while trying to create the guids: " + e);
    }

    try {
      String query = "~" + fieldName + " : ($gt: 0)";
      JSONArray result = client.execute(GNSCommand.selectQuery(query)).getResultJSONArray();
      for (int i = 0; i < result.length(); i++) {
        System.out.println(result.get(i).toString());
      }
      // best we can do should be at least 5, but possibly more objects in results
      Assert.assertThat(result.length(), Matchers.greaterThanOrEqualTo(5));
    } catch (ClientException | IOException | JSONException e) {
      Utils.failWithStackTrace("Exception executing selectQuery: " + e);
    }
  }

  /**
   * Check a query select with unreadable fields
   */
  @Test
  public void test_055_QuerySelectWorldNotReadable() {
    String fieldName = "testQueryWorldNotReadable";
    try {
      for (int cnt = 0; cnt < 5; cnt++) {
        String queryTestName = "queryTest-" + RandomString.randomString(12);
        client.execute(GNSCommand.createGUID(masterGuid, queryTestName));
        GuidEntry testEntry = GuidUtils.getGUIDKeys(queryTestName);
        // Remove default all fields / all guids ACL;
        client.execute(GNSCommand.aclRemove(AclAccessType.READ_WHITELIST, testEntry,
                GNSProtocol.ENTIRE_RECORD.toString(), GNSProtocol.ALL_GUIDS.toString()));
        CREATED_GUIDS.add(testEntry); // save them so we can delete them later
        JSONArray array = new JSONArray(Arrays.asList(25));
        client.execute(GNSCommand.fieldReplaceOrCreateList(testEntry.getGuid(), fieldName, array, testEntry));
      }
      waitSettle(WAIT_SETTLE);
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while tryin to create the guids: " + e);
    }
    try {
      JSONArray result = null;
      String query = "~" + fieldName + " : ($gt: 0)";
      result = client.execute(GNSCommand.selectQuery(query)).getResultJSONArray();
      for (int i = 0; i < result.length(); i++) {
        System.out.println(result.get(i).toString());
      }
      Assert.assertThat(result.length(), Matchers.equalTo(0));
    } catch (ClientException | IOException | JSONException e) {
      Utils.failWithStackTrace("Exception executing selectQuery: " + e);
    }
  }

  /**
   * Check a query select with a projection
   */
  @Test
  public void test_057_QuerySelectwithProjection() {
    String fieldName = "testQueryProjection";
    try {
      for (int cnt = 0; cnt < 5; cnt++) {
        String queryTestName = "queryTest-" + RandomString.randomString(12);
        client.execute(GNSCommand.createGUID(masterGuid, queryTestName));
        GuidEntry testEntry = GuidUtils.getGUIDKeys(queryTestName);
        // Remove default all fields / all guids ACL;
        client.execute(GNSCommand.aclRemove(AclAccessType.READ_WHITELIST, testEntry,
                GNSProtocol.ENTIRE_RECORD.toString(), GNSProtocol.ALL_GUIDS.toString()));
        CREATED_GUIDS.add(testEntry); // save them so we can delete them later
        JSONObject json = new JSONObject();
        json.put(fieldName, Arrays.asList(25));
        json.put("field1", "value1");
        client.execute(GNSCommand.update(testEntry.getGuid(), json, testEntry));
      }
      waitSettle(WAIT_SETTLE);
    } catch (ClientException | IOException | JSONException e) {
      Utils.failWithStackTrace("Exception while trying to create the guids: " + e);
    }

    try {
      String query = "~" + fieldName + " : ($gt: 0)";
      JSONArray result = waitForQueryResults(query);
      //JSONArray result = client.execute(GNSCommand.selectRecords(masterGuid, query, Arrays.asList(fieldName))).getResultJSONArray();
      for (int i = 0; i < result.length(); i++) {
        System.out.println(result.get(i).toString());
      }
      // best we can do should be at least 5, but possibly more objects in results
      Assert.assertThat(result.length(), Matchers.greaterThanOrEqualTo(5));
    } catch (JSONException e) {
      //} catch (ClientException | IOException | JSONException e) {
      Utils.failWithStackTrace("Exception executing selectRecords: " + e);
    }
  }

  /**
   * Check a query select with a projection of all fields
   */
  @Test
  public void test_058_QuerySelectwithProjectionAll() {
    String fieldName = "testQueryProjectionAll";
    try {
      for (int cnt = 0; cnt < 5; cnt++) {
        String queryTestName = "queryTest-" + RandomString.randomString(12);
        client.execute(GNSCommand.createGUID(masterGuid, queryTestName));
        GuidEntry testEntry = GuidUtils.getGUIDKeys(queryTestName);
        // Remove default all fields / all guids ACL;
        client.execute(GNSCommand.aclRemove(AclAccessType.READ_WHITELIST, testEntry,
                GNSProtocol.ENTIRE_RECORD.toString(), GNSProtocol.ALL_GUIDS.toString()));
        CREATED_GUIDS.add(testEntry); // save them so we can delete them later
        JSONObject json = new JSONObject();
        json.put(fieldName, Arrays.asList(25));
        json.put("field1", "value1");
        JSONObject subJson = new JSONObject();
        subJson.put("subfield", "subvalue1");
        json.put("nested", subJson);
        client.execute(GNSCommand.update(testEntry.getGuid(), json, testEntry));
      }
      waitSettle(WAIT_SETTLE);
    } catch (ClientException | IOException | JSONException e) {
      Utils.failWithStackTrace("Exception while trying to create the guids: " + e);
    }

    try {
      String query = "~" + fieldName + " : ($gt: 0)";
      JSONArray result = waitForQueryResults(query);
      //JSONArray result = client.execute(GNSCommand.selectRecords(masterGuid, query, null)).getResultJSONArray();
      for (int i = 0; i < result.length(); i++) {
        System.out.println(result.get(i).toString());
      }
      // best we can do should be at least 5, but possibly more objects in results
      Assert.assertThat(result.length(), Matchers.greaterThanOrEqualTo(5));
    } catch (JSONException e) {
      //} catch (ClientException | IOException | JSONException e) {
      Utils.failWithStackTrace("Exception executing selectRecords: " + e);
    }
  }

  /**
   * Check a query select with a projection
   */
  @Test
  public void test_059_QuerySelectwithProjectionSomeFieldsNotAccessible() {
    String fieldName = "testQueryProjectionSomeFieldsNotAccessible";
    try {
      for (int cnt = 0; cnt < 5; cnt++) {
        String queryTestName = "queryTest-" + RandomString.randomString(12);
        client.execute(GNSCommand.createGUID(masterGuid, queryTestName));
        GuidEntry testEntry = GuidUtils.getGUIDKeys(queryTestName);
        // Remove default all fields / all guids ACL;
        client.execute(GNSCommand.aclRemove(AclAccessType.READ_WHITELIST, testEntry,
                GNSProtocol.ENTIRE_RECORD.toString(), GNSProtocol.ALL_GUIDS.toString()));
        // Also remove masterGuid access to entire record
        client.execute(GNSCommand.aclRemove(AclAccessType.READ_WHITELIST, testEntry,
                GNSProtocol.ENTIRE_RECORD.toString(), masterGuid.getGuid()));
        CREATED_GUIDS.add(testEntry); // save them so we can delete them later
        JSONObject json = new JSONObject();
        json.put(fieldName, Arrays.asList(25));
        json.put("field1", "value1");
        json.put("inaccessableField", "someValue");
        client.execute(GNSCommand.update(testEntry.getGuid(), json, testEntry));
        // Add masterguid access to fieldName so that the query will work
        client.execute(GNSCommand.aclAdd(AclAccessType.READ_WHITELIST, testEntry,
                fieldName, masterGuid.getGuid()));
        // Add masterguid access to field1
        client.execute(GNSCommand.aclAdd(AclAccessType.READ_WHITELIST, testEntry,
                "field1", masterGuid.getGuid()));
        // Set up the ACL so the masterGuid can't read this field
        client.execute(GNSCommand.fieldCreateAcl(AclAccessType.READ_WHITELIST, testEntry,
                "inaccessableField", testEntry.getGuid()));
      }
      waitSettle(WAIT_SETTLE);
    } catch (ClientException | IOException | JSONException e) {
      Utils.failWithStackTrace("Exception while trying to create the guids: " + e);
    }
    try {
      String query = "~" + fieldName + " : ($gt: 0)";
      //JSONArray result = client.execute(GNSCommand.selectRecords(masterGuid, query, null)).getResultJSONArray();
      JSONArray result = waitForQueryResults(query);
      for (int i = 0; i < result.length(); i++) {
        System.out.println(result.get(i).toString());
      }
      // best we can do should be at least 5, but possibly more objects in results
      Assert.assertThat(result.length(), Matchers.greaterThanOrEqualTo(5));
      // Make sure that one of them has the field we want and
      Assert.assertTrue(result.getJSONObject(0).has("field1"));
      // doesn't have the field we can't see
      Assert.assertFalse(result.getJSONObject(0).has("inaccessableField"));
    } catch (JSONException e) {
      //} catch (ClientException | IOException | JSONException e) {
      Utils.failWithStackTrace("Exception executing selectRecords: " + e);
    }
  }

  private JSONArray waitForQueryResults(String query) {
    int cnt = 50;
    JSONArray result = null;
    do {
      try {
        result = client.execute(GNSCommand.selectRecords(masterGuid, query, null)).getResultJSONArray();
        if (result.length() >= 5) {
          return result;
        }
      } catch (ClientException | IOException e) {
        Utils.failWithStackTrace("Exception executing selectRecords: " + e);
        return result;
      }
      waitSettle(cnt > 10 ? WAIT_SETTLE : WAIT_SETTLE * 5);
    } while (cnt-- > 0);
    return result;
  }

  private static String createIndexTestField;

  /**
   * Create a test field
   */
  @Test
  public void test_060_CreateField() {
    createIndexTestField = "testField" + RandomString.randomString(6);
    try {
      client.execute(GNSCommand.fieldUpdate(masterGuid, createIndexTestField, createGeoJSONPolygon(AREA_EXTENT)));
      //clientCommands.fieldUpdate(masterGuid, createIndexTestField, createGeoJSONPolygon(AREA_EXTENT));
    } catch (JSONException | IOException | ClientException e) {
      Utils.failWithStackTrace("Exception during create field: " + e);
    }
  }

  //commnted out because fieldCreateIndex is protected in GNSCommand
//  /**
//   * Create an index
//   */
//  @Test
//  public void test_070_CreateIndex() {
//    try {
//      client.execute(GNSCommand.fieldCreateIndex(masterGuid, createIndexTestField, "2dsphere"));
//      //clientCommands.fieldCreateIndex(masterGuid, createIndexTestField, "2dsphere");
//    } catch (IOException | ClientException e) {
//      Utils.failWithStackTrace("Exception while creating index: " + e);
//    }
//  }
//
//  /**
//   * Check a query with the index
//   */
//  @Test
//  public void test_080_SelectPass() {
//    try {
//      waitSettle(WAIT_SETTLE);
//      JSONArray result = clientCommands.selectQuery(masterGuid, buildLocationQuery(createIndexTestField, AREA_EXTENT));
//      for (int i = 0; i < result.length(); i++) {
//        System.out.println(result.get(i).toString());
//      }
//      // best we can do should be at least 5, but possibly more objects in results
//      Assert.assertThat(result.length(), Matchers.greaterThanOrEqualTo(1));
//    } catch (JSONException | ClientException | IOException e) {
//      Utils.failWithStackTrace("Exception executing second selectQuery: " + e);
//    }
//  }
//
//  private static String dottedTestField;
//
//  /**
//   * Create a dotted field
//   */
//  @Test
//  public void test_081_CreateDottedField() {
//    dottedTestField = "testField" + RandomString.randomString(6) + ".subfield";
//    try {
//      clientCommands.fieldUpdate(masterGuid, dottedTestField, createGeoJSONPolygon(AREA_EXTENT));
//    } catch (JSONException | IOException | ClientException e) {
//      Utils.failWithStackTrace("Exception during create field: " + e);
//    }
//  }
//
//  /**
//   * Read back the dotted field
//   */
//  @Test
//  public void test_082_ReadDottedField() {
//    try {
//      String actual = clientCommands.fieldRead(masterGuid, dottedTestField);
//      JSONAssert.assertEquals(createGeoJSONPolygon(AREA_EXTENT).toString(), actual, JSONCompareMode.STRICT);
//    } catch (JSONException | IOException | ClientException e) {
//      Utils.failWithStackTrace("Exception during create field: " + e);
//    }
//  }
//
//  /**
//   * Check a select with a dotted field
//   */
//  @Test
//  public void test_083_SelectDottedField() {
//    try {
//      JSONArray result = clientCommands.selectQuery(masterGuid, buildLocationQuery(dottedTestField, AREA_EXTENT));
//      for (int i = 0; i < result.length(); i++) {
//        System.out.println(result.get(i).toString());
//      }
//      Assert.assertThat(result.length(), Matchers.greaterThanOrEqualTo(1));
//    } catch (JSONException | ClientException | IOException e) {
//      Utils.failWithStackTrace("Exception executing second selectNear: " + e);
//    }
//  }
//
//  /**
//   * Check a select with multiple clauses
//   */
//  @Test
//  public void test_084_SelectMultipleLocationsPass() {
//    try {
//      JSONArray result = clientCommands.selectQuery(masterGuid,
//              buildMultipleLocationsQuery(createIndexTestField, dottedTestField, AREA_EXTENT));
//      for (int i = 0; i < result.length(); i++) {
//        System.out.println(result.get(i).toString());
//      }
//      Assert.assertThat(result.length(), Matchers.greaterThanOrEqualTo(1));
//    } catch (JSONException | ClientException | IOException e) {
//      Utils.failWithStackTrace("Exception executing second selectNear: " + e);
//    }
//  }
  /**
   * Check an empty query
   */
  @Test
  public void test_090_EmptyQueryString() {
    try {
      String query = "";
      JSONArray result = client.execute(GNSCommand.selectQuery(query)).getResultJSONArray();
      for (int i = 0; i < result.length(); i++) {
        Assert.assertTrue(StringUtil.isValidGuidString(result.get(i).toString()));
      }
    } catch (IOException | JSONException | ClientException e) {
      Utils.failWithStackTrace("Exception executing empty query " + e);
    }
  }

  /**
   * Check an empty query
   */
  @Test
  public void test_091_EmptyQueryQuotes() {
    try {
      String query = "{}";
      JSONArray result = client.execute(GNSCommand.selectQuery(query)).getResultJSONArray();
      for (int i = 0; i < result.length(); i++) {
        Assert.assertTrue(StringUtil.isValidGuidString(result.get(i).toString()));
      }
    } catch (IOException | JSONException | ClientException e) {
      Utils.failWithStackTrace("Exception executing empty query " + e);
    }
  }

  /**
   * Check an empty query
   */
  @Test
  public void test_092_EmptyQueryBrackets() {
    try {
      String query = "[]";
      JSONArray result = client.execute(GNSCommand.selectQuery(query)).getResultJSONArray();
      for (int i = 0; i < result.length(); i++) {
        Assert.assertTrue(StringUtil.isValidGuidString(result.get(i).toString()));
      }
    } catch (IOException | JSONException | ClientException e) {
      Utils.failWithStackTrace("Exception executing empty query " + e);
    }
  }

  /**
   * Check an empty query
   */
  @Test
  public void test_093_MalformedJSONQuery() {
    try {
      String badQuery = "\"~money:{$gt:0";
      JSONArray result = client.execute(GNSCommand.selectQuery(badQuery)).getResultJSONArray();
      for (int i = 0; i < result.length(); i++) {
        Assert.assertTrue(StringUtil.isValidGuidString(result.get(i).toString()));
      }
    } catch (IOException | JSONException | ClientException e) {
      Utils.failWithStackTrace("Exception executing empty query " + e);
    }
  }

  /**
   * Check an empty query
   */
  @Test
  public void test_098_EvilOperators() {
    try {
      String query = "nr_valuesMap.secret:{$regex : ^i_like_cookies}";
      JSONArray result = client.execute(GNSCommand.selectQuery(query)).getResultJSONArray();
      //JSONArray result = clientCommands.selectQuery(query);
      for (int i = 0; i < result.length(); i++) {
        Assert.assertTrue(StringUtil.isValidGuidString(result.get(i).toString()));
      }
      Utils.failWithStackTrace("Should have throw an exception");
    } catch (IOException | JSONException e) {
      Utils.failWithStackTrace("Exception executing evil query " + e);
    } catch (ClientException e) {
      // Expected
    }
  }

  /**
   * Check an empty query
   */
  @Test
  public void test_099_EvilOperators2() {
    try {
      String query = "$where : \"this.nr_valuesMap.secret == 'i_like_cookies'\"";
      JSONArray result = client.execute(GNSCommand.selectQuery(query)).getResultJSONArray();
      //JSONArray result = clientCommands.selectQuery(query);
      for (int i = 0; i < result.length(); i++) {
        Assert.assertTrue(StringUtil.isValidGuidString(result.get(i).toString()));
      }
      Utils.failWithStackTrace("Should have throw an exception");
    } catch (IOException | JSONException e) {
      Utils.failWithStackTrace("Exception executing evil query " + e);
    } catch (ClientException e) {
      // Expected
    }
  }

  /**
   *
   */
  @Test
  public void test_999_SelectCleanup() {
    try {
      for (GuidEntry guid : CREATED_GUIDS) {
        client.execute(GNSCommand.removeGUID(masterGuid, guid.getGuid()));
        //clientCommands.guidRemove(masterGuid, guid.getGuid());
      }
      CREATED_GUIDS.clear();
      client.execute(GNSCommand.removeGUID(masterGuid, westyEntry.getGuid()));
      //clientCommands.guidRemove(masterGuid, westyEntry.getGuid());
      client.execute(GNSCommand.removeGUID(masterGuid, samEntry.getGuid()));
      //clientCommands.guidRemove(masterGuid, samEntry.getGuid());
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception during cleanup: " + e);
    }
  }

  // HELPER STUFF
  private static final String POLYGON = "Polygon";
  private static final String COORDINATES = "coordinates";
  private static final String TYPE = "type";

  private static JSONObject createGeoJSONPolygon(List<Point2D> coordinates) throws JSONException {
    JSONArray ring = new JSONArray();
    for (int i = 0; i < coordinates.size(); i++) {
      ring.put(i, new JSONArray(
              Arrays.asList(coordinates.get(i).getX(), coordinates.get(i).getY())));
    }
    JSONArray jsonCoordinates = new JSONArray();
    jsonCoordinates.put(0, ring);
    JSONObject json = new JSONObject();
    json.put(TYPE, POLYGON);
    json.put(COORDINATES, jsonCoordinates);
    return json;
  }

  private static final double LEFT = -98.08;
  private static final double RIGHT = -96.01;
  private static final double TOP = 33.635;
  private static final double BOTTOM = 31.854;

  private static final Point2D UPPER_LEFT = new Point2D.Double(LEFT, TOP);
  //private static final GlobalCoordinate UPPER_LEFT = new GlobalCoordinate(33.45, -98.08);
  private static final Point2D UPPER_RIGHT = new Point2D.Double(RIGHT, TOP);
  //private static final GlobalCoordinate UPPER_RIGHT = new GlobalCoordinate(33.45, -96.01);
  private static final Point2D LOWER_RIGHT = new Point2D.Double(RIGHT, BOTTOM);
  //private static final GlobalCoordinate LOWER_RIGHT = new GlobalCoordinate(32.23, -96.01);
  private static final Point2D LOWER_LEFT = new Point2D.Double(LEFT, BOTTOM);
  //private static final GlobalCoordinate LOWER_LEFT = new GlobalCoordinate(32.23, -98.08);

  private static final List<Point2D> AREA_EXTENT = new ArrayList<>(
          Arrays.asList(UPPER_LEFT, UPPER_RIGHT, LOWER_RIGHT, LOWER_LEFT, UPPER_LEFT));

  private static String buildLocationQuery(String locationField, List<Point2D> coordinates) throws JSONException {
    return "~" + locationField + ":{"
            + "$geoIntersects :{"
            + "$geometry:"
            + createGeoJSONPolygon(coordinates).toString()
            + "}"
            + "}";
  }

  private static String buildMultipleLocationsQuery(String locationField1, String locationField2, List<Point2D> coordinates) throws JSONException {
    return buildOrQuery(buildLocationQuery(locationField1, coordinates),
            buildLocationQuery(locationField2, coordinates)
    );
  }

  private static String buildOrQuery(String... clauses) {
    StringBuilder result = new StringBuilder();
    String prefix = "";
    result.append("$or: [");
    for (String clause : clauses) {
      result.append(prefix);
      result.append("{");
      result.append(clause);
      result.append("}");
      prefix = ",";
    }
    result.append("]");
    return result.toString();
  }

  private static void waitSettle(long wait) {
    try {
      if (wait > 0) {
        Thread.sleep(wait);
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

}
