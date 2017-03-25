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
import edu.umass.cs.gnscommon.utils.ThreadUtils;
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

  private static GNSClientCommands clientCommands = null;
  private static GuidEntry masterGuid;
  private static GuidEntry westyEntry;
  private static GuidEntry samEntry;
  private static final Set<GuidEntry> createdGuids = new HashSet<>();

  /**
   *
   */
  public SelectTest() {
    if (clientCommands == null) {
      try {
        clientCommands = new GNSClientCommands();
        clientCommands.setForceCoordinatedReads(true);
      } catch (IOException e) {
        Utils.failWithStackTrace("Exception creating client: " + e);
      }
      try {
        masterGuid = GuidUtils.getGUIDKeys(globalAccountName);
      } catch (Exception e) {
        Utils.failWithStackTrace("Exception while creating account guid: " + e);
      }
    }
  }

  /**
   * Create the guids
   */
  @Test
  public void test_10_CreateGuids() {
    try {
      westyEntry = clientCommands.guidCreate(masterGuid, "westy" + RandomString.randomString(12));
      samEntry = clientCommands.guidCreate(masterGuid, "sam" + RandomString.randomString(12));
      System.out.println("Created: " + westyEntry);
      System.out.println("Created: " + samEntry);
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception registering guids: " + e);
    }
  }

  /**
   * Create the fields
   */
  @Test
  public void test_20_cats() {
    try {
      clientCommands.fieldCreateOneElementList(westyEntry.getGuid(), "cats", "whacky", westyEntry);

      Assert.assertEquals("whacky",
              clientCommands.fieldReadArrayFirstElement(westyEntry.getGuid(), "cats", westyEntry));

      clientCommands.fieldAppendWithSetSemantics(westyEntry.getGuid(), "cats", new JSONArray(
              Arrays.asList("hooch", "maya", "red", "sox", "toby")), westyEntry);

      HashSet<String> expected = new HashSet<>(Arrays.asList("hooch",
              "maya", "red", "sox", "toby", "whacky"));
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(clientCommands
              .fieldReadArray(westyEntry.getGuid(), "cats", westyEntry));
      Assert.assertEquals(expected, actual);

      clientCommands.fieldClear(westyEntry.getGuid(), "cats", new JSONArray(
              Arrays.asList("maya", "toby")), westyEntry);
      expected = new HashSet<>(Arrays.asList("hooch", "red", "sox",
              "whacky"));
      actual = JSONUtils.JSONArrayToHashSet(clientCommands.fieldReadArray(
              westyEntry.getGuid(), "cats", westyEntry));
      Assert.assertEquals(expected, actual);

      clientCommands.fieldReplaceFirstElement(westyEntry.getGuid(), "cats", "maya", westyEntry);
      Assert.assertEquals("maya",
              clientCommands.fieldReadArrayFirstElement(westyEntry.getGuid(), "cats", westyEntry));

      clientCommands.fieldAppendWithSetSemantics(westyEntry.getGuid(), "cats", "fred", westyEntry);
      expected = new HashSet<>(Arrays.asList("maya", "fred"));
      actual = JSONUtils.JSONArrayToHashSet(clientCommands.fieldReadArray(
              westyEntry.getGuid(), "cats", westyEntry));
      Assert.assertEquals(expected, actual);

      clientCommands.fieldAppendWithSetSemantics(westyEntry.getGuid(), "cats", "fred", westyEntry);
      expected = new HashSet<>(Arrays.asList("maya", "fred"));
      actual = JSONUtils.JSONArrayToHashSet(clientCommands.fieldReadArray(
              westyEntry.getGuid(), "cats", westyEntry));
      Assert.assertEquals(expected, actual);
    } catch (IOException | ClientException | JSONException e) {
      Utils.failWithStackTrace("Exception when we were not expecting testing DB: " + e);
    }
  }

  /**
   * Check the basic field select command
   */
  @Test
  public void test_30_BasicSelect() {
    try {
      JSONArray result = clientCommands.select(masterGuid, "cats", "fred");
      // best we can do since there will be one, but possibly more objects in results
      Assert.assertThat(result.length(), Matchers.greaterThanOrEqualTo(1));
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception when we were not expecting it: " + e);
    }
  }

  /**
   * Check the basic field select command with world readable records
   */
  @Test
  public void test_31_BasicSelectWorldReadable() {
    try {
      JSONArray result = clientCommands.select("cats", "fred");
      // best we can do since there will be one, but possibly more objects in results
      Assert.assertThat(result.length(), Matchers.greaterThanOrEqualTo(1));
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception when we were not expecting it: " + e);
    }
  }

  /**
   * Check a near and within commands
   */
  @Test
  public void test_40_GeoSpatialSelect() {
    try {
      for (int cnt = 0; cnt < 5; cnt++) {
        GuidEntry testEntry = clientCommands.guidCreate(masterGuid, "geoTest-" + RandomString.randomString(12));
        createdGuids.add(testEntry); // save them so we can delete them later
        clientCommands.setLocation(testEntry, 0.0, 0.0);
      }
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception when we were not expecting it: " + e);
    }

    try {

      JSONArray loc = new JSONArray();
      loc.put(1.0);
      loc.put(1.0);
      JSONArray result = clientCommands.selectNear(masterGuid, GNSProtocol.LOCATION_FIELD_NAME.toString(), loc, 2000000.0);
      // best we can do should be at least 5, but possibly more objects in results
      Assert.assertThat(result.length(), Matchers.greaterThanOrEqualTo(5));
    } catch (JSONException | ClientException | IOException e) {
      Utils.failWithStackTrace("Exception executing selectNear: " + e);
    }

    try {

      JSONArray rect = new JSONArray();
      JSONArray upperLeft = new JSONArray();
      upperLeft.put(1.0);
      upperLeft.put(1.0);
      JSONArray lowerRight = new JSONArray();
      lowerRight.put(-1.0);
      lowerRight.put(-1.0);
      rect.put(upperLeft);
      rect.put(lowerRight);
      JSONArray result = clientCommands.selectWithin(masterGuid, GNSProtocol.LOCATION_FIELD_NAME.toString(), rect);
      // best we can do should be at least 5, but possibly more objects in results
      Assert.assertThat(result.length(), Matchers.greaterThanOrEqualTo(5));
    } catch (JSONException | ClientException | IOException e) {
      Utils.failWithStackTrace("Exception executing selectWithin: " + e);
    }
  }

  /**
   * Check a query select with a reader
   */
  @Test
  public void test_50_QuerySelectwithReader() {
    String fieldName = "testQuery";
    try {
      for (int cnt = 0; cnt < 5; cnt++) {
        GuidEntry testEntry = clientCommands.guidCreate(masterGuid, "queryTest-" + RandomString.randomString(12));
        // Remove default all fields / all guids ACL;
        clientCommands.aclRemove(AclAccessType.READ_WHITELIST, testEntry,
                GNSProtocol.ENTIRE_RECORD.toString(), GNSProtocol.ALL_GUIDS.toString());
        createdGuids.add(testEntry); // save them so we can delete them later
        JSONArray array = new JSONArray(Arrays.asList(25));
        clientCommands.fieldReplaceOrCreateList(testEntry.getGuid(), fieldName, array, testEntry);
      }
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while tryint to create the guids: " + e);
    }

    try {
      String query = "~" + fieldName + " : ($gt: 0)";
      JSONArray result = clientCommands.selectQuery(masterGuid, query);
      for (int i = 0; i < result.length(); i++) {
        System.out.println(result.get(i).toString());
      }
      // best we can do should be at least 5, but possibly more objects in results
      Assert.assertThat(result.length(), Matchers.greaterThanOrEqualTo(5));
    } catch (ClientException | IOException | JSONException e) {
      Utils.failWithStackTrace("Exception executing selectNear: " + e);
    }

    try {

      JSONArray rect = new JSONArray();
      JSONArray upperLeft = new JSONArray();
      upperLeft.put(1.0);
      upperLeft.put(1.0);
      JSONArray lowerRight = new JSONArray();
      lowerRight.put(-1.0);
      lowerRight.put(-1.0);
      rect.put(upperLeft);
      rect.put(lowerRight);
      JSONArray result = clientCommands.selectWithin(masterGuid, GNSProtocol.LOCATION_FIELD_NAME.toString(), rect);
      // best we can do should be at least 5, but possibly more objects in results
      Assert.assertThat(result.length(), Matchers.greaterThanOrEqualTo(5));
    } catch (JSONException | ClientException | IOException e) {
      Utils.failWithStackTrace("Exception executing selectWithin: " + e);
    }
  }

  /**
   * Check a query select with world readable fields
   */
  @Test
  public void test_51_QuerySelectWorldReadable() {
    String fieldName = "testQueryWorldReadable";
    try {
      for (int cnt = 0; cnt < 5; cnt++) {
        GuidEntry testEntry = clientCommands.guidCreate(masterGuid, "queryTest-" + RandomString.randomString(12));
        createdGuids.add(testEntry); // save them so we can delete them later
        JSONArray array = new JSONArray(Arrays.asList(25));
        clientCommands.fieldReplaceOrCreateList(testEntry.getGuid(), fieldName, array, testEntry);
      }
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while tryint to create the guids: " + e);
    }

    try {
      String query = "~" + fieldName + " : ($gt: 0)";
      JSONArray result = clientCommands.selectQuery(query);
      for (int i = 0; i < result.length(); i++) {
        System.out.println(result.get(i).toString());
      }
      // best we can do should be at least 5, but possibly more objects in results
      Assert.assertThat(result.length(), Matchers.greaterThanOrEqualTo(5));
    } catch (ClientException | IOException | JSONException e) {
      Utils.failWithStackTrace("Exception executing selectNear: " + e);
    }

    try {

      JSONArray rect = new JSONArray();
      JSONArray upperLeft = new JSONArray();
      upperLeft.put(1.0);
      upperLeft.put(1.0);
      JSONArray lowerRight = new JSONArray();
      lowerRight.put(-1.0);
      lowerRight.put(-1.0);
      rect.put(upperLeft);
      rect.put(lowerRight);
      JSONArray result = clientCommands.selectWithin(GNSProtocol.LOCATION_FIELD_NAME.toString(), rect);
      // best we can do should be at least 5, but possibly more objects in results
      Assert.assertThat(result.length(), Matchers.greaterThanOrEqualTo(5));
    } catch (JSONException | ClientException | IOException e) {
      Utils.failWithStackTrace("Exception executing selectWithin: " + e);
    }
  }

  /**
   * Check a query select with unreadable fields
   */
  @Test
  public void test_52_QuerySelectWorldNotReadable() {
    String fieldName = "testQueryWorldNotReadable";
    try {
      for (int cnt = 0; cnt < 5; cnt++) {
        GuidEntry testEntry = clientCommands.guidCreate(masterGuid, "queryTest-" + RandomString.randomString(12));
        // Remove default all fields / all guids ACL;
        clientCommands.aclRemove(AclAccessType.READ_WHITELIST, testEntry,
                GNSProtocol.ENTIRE_RECORD.toString(), GNSProtocol.ALL_GUIDS.toString());
        createdGuids.add(testEntry); // save them so we can delete them later
        JSONArray array = new JSONArray(Arrays.asList(25));
        clientCommands.fieldReplaceOrCreateList(testEntry.getGuid(), fieldName, array, testEntry);
      }
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while tryin to create the guids: " + e);
    }
    try {
      JSONArray result = null;
//      int retries = 10;
//      do {
      String query = "~" + fieldName + " : ($gt: 0)";
      result = clientCommands.selectQuery(query);
      for (int i = 0; i < result.length(); i++) {
        System.out.println(result.get(i).toString());
      }
      ThreadUtils.sleep(100);
      //} while (retries-- > 0 && result.length() != 0);
      //Assert.assertNotNull(result);
      // Should return none because we can't see them
      Assert.assertThat(result.length(), Matchers.equalTo(0));
    } catch (ClientException | IOException | JSONException e) {
      Utils.failWithStackTrace("Exception executing selectQuery: " + e);
    }
  }

  private static String createIndexTestField;

  /**
   * Create a test field
   */
  @Test
  public void test_60_CreateField() {
    createIndexTestField = "testField" + RandomString.randomString(6);
    try {
      clientCommands.fieldUpdate(masterGuid, createIndexTestField, createGeoJSONPolygon(AREA_EXTENT));
    } catch (JSONException | IOException | ClientException e) {
      Utils.failWithStackTrace("Exception during create field: " + e);
    }
  }

  /**
   * Create an index
   */
  @Test
  public void test_70_CreateIndex() {
    try {
      clientCommands.fieldCreateIndex(masterGuid, createIndexTestField, "2dsphere");
    } catch (IOException | ClientException e) {
      Utils.failWithStackTrace("Exception while creating index: " + e);
    }
  }

  /**
   * Check a query with the index
   */
  @Test
  public void test_80_SelectPass() {
    try {
      JSONArray result = clientCommands.selectQuery(masterGuid, buildLocationQuery(createIndexTestField, AREA_EXTENT));
      for (int i = 0; i < result.length(); i++) {
        System.out.println(result.get(i).toString());
      }
      // best we can do should be at least 5, but possibly more objects in results
      Assert.assertThat(result.length(), Matchers.greaterThanOrEqualTo(1));
    } catch (JSONException | ClientException | IOException e) {
      Utils.failWithStackTrace("Exception executing second selectNear: " + e);
    }
  }

  private static String dottedTestField;

  /**
   * Create a dotted field
   */
  @Test
  public void test_81_CreateDottedField() {
    dottedTestField = "testField" + RandomString.randomString(6) + ".subfield";
    try {
      clientCommands.fieldUpdate(masterGuid, dottedTestField, createGeoJSONPolygon(AREA_EXTENT));
    } catch (JSONException | IOException | ClientException e) {
      Utils.failWithStackTrace("Exception during create field: " + e);
    }
  }

  /**
   * Read back the dotted field
   */
  @Test
  public void test_82_ReadDottedField() {
    try {
      String actual = clientCommands.fieldRead(masterGuid, dottedTestField);
      JSONAssert.assertEquals(createGeoJSONPolygon(AREA_EXTENT).toString(), actual, JSONCompareMode.STRICT);
    } catch (JSONException | IOException | ClientException e) {
      Utils.failWithStackTrace("Exception during create field: " + e);
    }
  }

  /**
   * Check a select with a dotted field
   */
  @Test
  public void test_83_SelectDottedField() {
    try {
      JSONArray result = clientCommands.selectQuery(masterGuid, buildLocationQuery(dottedTestField, AREA_EXTENT));
      for (int i = 0; i < result.length(); i++) {
        System.out.println(result.get(i).toString());
      }
      Assert.assertThat(result.length(), Matchers.greaterThanOrEqualTo(1));
    } catch (JSONException | ClientException | IOException e) {
      Utils.failWithStackTrace("Exception executing second selectNear: " + e);
    }
  }

  /**
   * Check a select with multiple clauses
   */
  @Test
  public void test_84_SelectMultipleLocationsPass() {
    try {
      JSONArray result = clientCommands.selectQuery(masterGuid,
              buildMultipleLocationsQuery(createIndexTestField, dottedTestField, AREA_EXTENT));
      for (int i = 0; i < result.length(); i++) {
        System.out.println(result.get(i).toString());
      }
      Assert.assertThat(result.length(), Matchers.greaterThanOrEqualTo(1));
    } catch (JSONException | ClientException | IOException e) {
      Utils.failWithStackTrace("Exception executing second selectNear: " + e);
    }
  }

  /**
   * Check an empty query
   */
  @Test
  public void test_90_EmptyQuery() {
    try {
      String query = "";
      JSONArray result = clientCommands.selectQuery(query);
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
  public void test_98_EvilOperators() {
    try {
      String query = "nr_valuesMap.secret:{$regex : ^i_like_cookies}";
      JSONArray result = clientCommands.selectQuery(query);
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
  public void test_99_EvilOperators2() {
    try {
      String query = "$where : \"this.nr_valuesMap.secret == 'i_like_cookies'\"";
      JSONArray result = clientCommands.selectQuery(query);
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
      for (GuidEntry guid : createdGuids) {
        clientCommands.guidRemove(masterGuid, guid.getGuid());
      }
      createdGuids.clear();
      clientCommands.guidRemove(masterGuid, westyEntry.getGuid());
      clientCommands.guidRemove(masterGuid, samEntry.getGuid());
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

}
