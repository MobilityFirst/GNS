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
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.utils.RandomString;

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
   *
   */
  @Test
  public void test_01_CreateGuids() {
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
   *
   */
  @Test
  public void test_02_cats() {
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
   *
   */
  @Test
  public void test_03_BasicSelect() {
    try {
      JSONArray result = clientCommands.select("cats", "fred");
      // best we can do since there will be one, but possibly more objects in results
      Assert.assertThat(result.length(), Matchers.greaterThanOrEqualTo(1));
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception when we were not expecting it: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_04_GeoSpatialSelect() {
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
      JSONArray result = clientCommands.selectNear(GNSProtocol.LOCATION_FIELD_NAME.toString(), loc, 2000000.0);
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
      JSONArray result = clientCommands.selectWithin(GNSProtocol.LOCATION_FIELD_NAME.toString(), rect);
      // best we can do should be at least 5, but possibly more objects in results
      Assert.assertThat(result.length(), Matchers.greaterThanOrEqualTo(5));
    } catch (JSONException | ClientException | IOException e) {
      Utils.failWithStackTrace("Exception executing selectWithin: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_05_QuerySelect() {
    String fieldName = "testQuery";
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

  private static String createIndexTestField;

  /**
   *
   */
  @Test
  public void test_06_CreateField() {
    createIndexTestField = "testField" + RandomString.randomString(6);
    try {
      clientCommands.fieldUpdate(masterGuid, createIndexTestField, createGeoJSONPolygon(AREA_EXTENT));
    } catch (JSONException | IOException | ClientException e) {
      Utils.failWithStackTrace("Exception during create field: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_07_CreateIndex() {
    try {
      clientCommands.fieldCreateIndex(masterGuid, createIndexTestField, "2dsphere");
    } catch (IOException | ClientException e) {
      Utils.failWithStackTrace("Exception while creating index: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_08_SelectPass() {
    try {
      JSONArray result = clientCommands.selectQuery(buildQuery(createIndexTestField, AREA_EXTENT));
      for (int i = 0; i < result.length(); i++) {
        System.out.println(result.get(i).toString());
      }
      // best we can do should be at least 5, but possibly more objects in results
      Assert.assertThat(result.length(), Matchers.greaterThanOrEqualTo(1));
    } catch (JSONException | ClientException | IOException e) {
      Utils.failWithStackTrace("Exception executing second selectNear: " + e);
    }
  }
  
  /**
   *
   */
  @Test
  public void test_09_SelectCleanup() {
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

  private static String buildQuery(String locationField, List<Point2D> coordinates) throws JSONException {
    return "~" + locationField + ":{"
            + "$geoIntersects :{"
            + "$geometry:"
            + createGeoJSONPolygon(coordinates).toString()
            + "}"
            + "}";
  }
  
}
