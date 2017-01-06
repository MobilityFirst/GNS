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
import edu.umass.cs.gnscommon.GNSProtocol;
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
import org.junit.Test;
import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;
import org.junit.Assert;

/**
 *
 * @author westy
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class HttpClientTest {

  private static final String ACCOUNT_ALIAS = "test@gns.name"; // REPLACE THIS WITH YOUR ACCOUNT ALIAS
  private static final String PASSWORD = "password";

  private HttpClient client;

  private static GuidEntry masterGuid;

  public HttpClientTest() {
    if (client == null) {
      // This should look up the appropriate port
      client = new HttpClient("127.0.0.1", 24703);
    }
  }
  
  @Test
  public void test_899_Http_CreateAccountGuid() {
    try {
      masterGuid = GuidUtils.lookupOrCreateAccountGuid(client, ACCOUNT_ALIAS, PASSWORD, true);

    } catch (Exception e) {
      failWithStackTrace("Exception while creating master guid: ", e);
    }
  }

  /**
   *
   */
  @Test
  public void test_900_Http_LookupGuid() {
    try {
      Assert.assertEquals(masterGuid.getGuid(), client.lookupGuid(ACCOUNT_ALIAS));
    } catch (IOException | ClientException e) {
      failWithStackTrace("Exception in LookupGuid: ", e);
    }
  }

  private static GuidEntry httpOneEntry;
  private static GuidEntry httpTwoEntry;

  /**
   *
   */
  @Test
  public void test_901_Http_CreateGuids() {
    try {
      httpOneEntry = client.guidCreate(masterGuid, "httpOneEntry" + RandomString.randomString(6));
      httpTwoEntry = client.guidCreate(masterGuid, "httpTwoEntry" + RandomString.randomString(6));
      System.out.println("Created: " + httpOneEntry);
      System.out.println("Created: " + httpTwoEntry);
    } catch (IOException | ClientException | NoSuchAlgorithmException e) {
      failWithStackTrace("Exception in Http_CreateFields: ", e);
    }
  }

  @Test
  public void test_902_Http_RemoveACL() {
    try {
      // remove default read acces for this test
      client.aclRemove(AclAccessType.READ_WHITELIST, httpOneEntry,
              GNSProtocol.ENTIRE_RECORD.toString(), GNSProtocol.ALL_GUIDS.toString());
    } catch (IOException | ClientException e) {
      failWithStackTrace("Exception in Http_RemoveACL: ", e);
    }
  }

  @Test
  public void test_910_Http_UpdateFields() {
    try {
      client.fieldUpdate(httpOneEntry.getGuid(), "environment", "work", httpOneEntry);
      client.fieldUpdate(httpOneEntry.getGuid(), "ssn", "000-00-0000", httpOneEntry);
      client.fieldUpdate(httpOneEntry.getGuid(), "password", "666flapJack", httpOneEntry);
      client.fieldUpdate(httpOneEntry.getGuid(), "address", "100 Hinkledinkle Drive", httpOneEntry);
    } catch (IOException | ClientException | JSONException e) {
      failWithStackTrace("Exception in Http_UpdateFields: ", e);
    }
  }

  @Test
  public void test_911_Http_CheckFields() {
    try {
      // read my own field
      Assert.assertEquals("work",
              client.fieldRead(httpOneEntry.getGuid(), "environment", httpOneEntry));
      // read another field
      Assert.assertEquals("000-00-0000",
              client.fieldRead(httpOneEntry.getGuid(), "ssn", httpOneEntry));
      // read another field
      Assert.assertEquals("666flapJack",
              client.fieldRead(httpOneEntry.getGuid(), "password", httpOneEntry));
    } catch (IOException | ClientException e) {
      failWithStackTrace("Exception in Http_CheckFields: ", e);
    }
  }

  @Test
  public void test_913_Http_CheckFieldsFail() {
    try {
      try {
        String result = client.fieldRead(httpOneEntry.getGuid(), "environment", httpTwoEntry);
        failWithStackTrace("Result of read of httpOneEntry's environment by httpTwoEntry is " + result
                + " which is wrong because it should have been rejected.");
      } catch (ClientException e) {
      } catch (IOException e) {
        failWithStackTrace("Exception during read of westy's environment by sam: ", e);
      }
    } catch (Exception e) {
      failWithStackTrace("Exception in Http_CheckFieldsFail: ", e);
    }
  }

  @Test
  public void test_920_Http_ACLAdd() {
    try {
      System.out.println("Using:" + httpOneEntry);
      System.out.println("Using:" + httpTwoEntry);
      try {
        client.aclAdd(AclAccessType.READ_WHITELIST, httpOneEntry, "environment",
                httpTwoEntry.getGuid());
      } catch (IOException | ClientException e) {
        failWithStackTrace("Exception adding Sam to Westy's readlist: ", e);
        e.printStackTrace();
      }
    } catch (Exception e) {
      failWithStackTrace("Exception in Http_ACLAdd: ", e);
    }
  }

  @Test
  public void test_921_Http_CheckAccess() {
    try {
      try {
        Assert.assertEquals("work",
                client.fieldRead(httpOneEntry.getGuid(), "environment", httpTwoEntry));
      } catch (IOException | ClientException e) {
        failWithStackTrace("Exception while Sam reading Westy's field: ", e);
      }
    } catch (Exception e) {
      failWithStackTrace("Exception in Http_CheckAccess: ", e);
    }
  }
  
   /**
   *
   */
  @Test
  public void test_932_Update() {
    try {
      JSONObject json = new JSONObject();
      JSONObject subJson = new JSONObject();
      subJson.put("sally", "red");
      subJson.put("sammy", "green");
      JSONObject subsubJson = new JSONObject();
      subsubJson.put("right", "seven");
      subsubJson.put("left", "eight");
      subJson.put("sally", subsubJson);
      json.put("flapjack", subJson);
      client.update(httpOneEntry, json);
    } catch (JSONException | IOException | ClientException e) {
      failWithStackTrace("Exception while adding field \"flapjack\": ", e);
    }
  }

  @Test
  public void test_933_ReadAll() {
    try {
      JSONObject expected = new JSONObject();
      JSONObject subJson = new JSONObject();
      subJson.put("sammy", "green");
      JSONObject subsubJson = new JSONObject();
      subsubJson.put("right", "seven");
      subsubJson.put("left", "eight");
      subJson.put("sally", subsubJson);
      expected.put("flapjack", subJson);
      expected.put("environment", "work");
      expected.put("ssn", "000-00-0000");
      expected.put("password", "666flapJack");
      expected.put("address", "100 Hinkledinkle Drive");
      JSONObject actual = client.read(httpOneEntry);
      JSONAssert.assertEquals(expected, actual, JSONCompareMode.NON_EXTENSIBLE);
      //System.out.println(actual);
    } catch (JSONException | IOException | ClientException e) {
      failWithStackTrace("Exception while reading JSON: ", e);
    }
  }

  @Test
  public void test_934_ReadDeep() {
    try {
      String actual = client.fieldRead(httpOneEntry.getGuid(), "flapjack.sally.right", httpOneEntry);
      Assert.assertEquals("seven", actual);
    } catch (IOException | ClientException e) {
      failWithStackTrace("Exception while reading \"flapjack.sally.right\": ", e);
    }
  }

  @Test
  public void test_935_ReadMid() {
    try {
      String actual = client.fieldRead(httpOneEntry.getGuid(), "flapjack.sally", httpOneEntry);
      String expected = "{ \"left\" : \"eight\" , \"right\" : \"seven\"}";
      //System.out.println("expected:" + expected);
      //System.out.println("actual:" + actual);
      JSONAssert.assertEquals(expected, actual, JSONCompareMode.NON_EXTENSIBLE);
    } catch (IOException | ClientException | JSONException e) {
      failWithStackTrace("Exception while reading \"flapjack.sally\": ", e);
    }
  }

  @Test
  public void test_936_ReadShallow() {
    try {
      String actual = client.fieldRead(httpOneEntry.getGuid(), "flapjack", httpOneEntry);
      String expected = "{ \"sammy\" : \"green\" , \"sally\" : { \"left\" : \"eight\" , \"right\" : \"seven\"}}";
      //System.out.println("expected:" + expected);
      //System.out.println("actual:" + actual);
      JSONAssert.assertEquals(expected, actual, JSONCompareMode.NON_EXTENSIBLE);
    } catch (IOException | ClientException | JSONException e) {
      failWithStackTrace("Exception while reading \"flapjack\": ", e);
    }
  }

  @Test
  public void test_951_Http_createCats() {
    try {
      client.fieldCreateSingleElementList(httpOneEntry.getGuid(), "cats", "whacky", httpOneEntry);
    } catch (IOException | ClientException e) {
      failWithStackTrace("Exception when we were not expecting testing cats: ", e);
    }
  }

  @Test
  public void test_952_Http_testCats() {
    try {
      Assert.assertEquals("whacky",
              client.fieldReadFirstElement(httpOneEntry.getGuid(), "cats", httpOneEntry));
    } catch (IOException | ClientException e) {
      failWithStackTrace("Exception when we were not expecting testing cats: ", e);
    }
  }

  @Test
  public void test_953_Http_createMoreCats() {
    try {
      client.fieldAppendWithSetSemantics(httpOneEntry.getGuid(), "cats", new JSONArray(
              Arrays.asList("hooch", "maya", "red", "sox", "toby")), httpOneEntry);

    } catch (IOException | ClientException e) {
      failWithStackTrace("Exception in createMoreCats: ", e);
    }
  }

  @Test
  public void test_954_Http_checkMoreCats() {
    try {
      HashSet<String> expected = new HashSet<>(Arrays.asList("hooch",
              "maya", "red", "sox", "toby", "whacky"));
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(client.fieldReadArray(httpOneEntry.getGuid(), 
              "cats", httpOneEntry));
      Assert.assertEquals(expected, actual);

    } catch (IOException | ClientException | JSONException e) {
      failWithStackTrace("Exception in checkMoreCats: ", e);
    }
  }

  @Test
  public void test_955_Http_clearCats() {
    try {
      client.fieldClear(httpOneEntry.getGuid(), "cats", new JSONArray(
              Arrays.asList("maya", "toby")), httpOneEntry);
    } catch (IOException | ClientException e) {
      failWithStackTrace("Exception in clearCats: ", e);
    }
  }

  @Test
  public void test_956_Http_checkClearCats() {
    try {
      HashSet<String> expected = new HashSet<>(Arrays.asList("hooch", "red", "sox",
              "whacky"));
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(client.fieldReadArray(
              httpOneEntry.getGuid(), "cats", httpOneEntry));
      Assert.assertEquals(expected, actual);

    } catch (IOException | ClientException | JSONException e) {
      failWithStackTrace("Exception in checkClearCats: ", e);
    }
  }

  @Test
  public void test_957_Http_createEvenMoreCats() {
    try {
      client.fieldAppendWithSetSemantics(httpOneEntry.getGuid(), "cats", "fred", httpOneEntry);
    } catch (IOException | ClientException e) {
      failWithStackTrace("Exception in createEvenMoreCats: ", e);
    }
  }

  @Test
  public void test_958_Http_checkEvenMoreCats() {
    try {
      client.fieldAppendWithSetSemantics(httpOneEntry.getGuid(), "cats", "fred", httpOneEntry);
       HashSet<String> expected = new HashSet<>(Arrays.asList("hooch", "red", "sox",
              "whacky", "fred"));
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(client.fieldReadArray(
              httpOneEntry.getGuid(), "cats", httpOneEntry));
      Assert.assertEquals(expected, actual);
    } catch (IOException | ClientException | JSONException e) {
      failWithStackTrace("Exception in checkEvenMoreCats: ", e);
    }
  }

  /**
   *
   */
  @Test
  public void test_960_Http_BasicSelect() {
    try {
      JSONArray result = client.select("cats", "fred");
      // best we can do since there will be one, but possibly more objects in results
      Assert.assertThat(result.length(), greaterThanOrEqualTo(1));
    } catch (IOException | ClientException | JSONException e) {
      failWithStackTrace("Exception when we were not expecting it: ", e);
    }
  }

  /**
   *
   */
  @Test
  public void test_961_Http_GeoSpatialSelect() {
    try {
      for (int cnt = 0; cnt < 5; cnt++) {
        GuidEntry testEntry = client.guidCreate(masterGuid, "geoTest-" + RandomString.randomString(6));
        client.setLocation(0.0, 0.0, testEntry);
      }
    } catch (IOException | ClientException | NoSuchAlgorithmException e) {
      failWithStackTrace("Exception when we were not expecting it: ", e);
    }

    try {

      JSONArray loc = new JSONArray();
      loc.put(1.0);
      loc.put(1.0);
      JSONArray result = client.selectNear(GNSProtocol.LOCATION_FIELD_NAME.toString(), loc, 2000000.0);
      // best we can do should be at least 5, but possibly more objects in results
      Assert.assertThat(result.length(), greaterThanOrEqualTo(5));
    } catch (JSONException | IOException | ClientException e) {
      failWithStackTrace("Exception executing selectNear: ", e);
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
      JSONArray result = client.selectWithin(GNSProtocol.LOCATION_FIELD_NAME.toString(), rect);
      // best we can do should be at least 5, but possibly more objects in results
      Assert.assertThat(result.length(), greaterThanOrEqualTo(5));
    } catch (JSONException | IOException | ClientException e) {
      failWithStackTrace("Exception executing selectWithin: ", e);
    }
  }

  /**
   *
   */
  @Test
  public void test_962_Http_QuerySelect() {
    String fieldName = "testQuery";
    try {
      for (int cnt = 0; cnt < 5; cnt++) {
        GuidEntry testEntry = client.guidCreate(masterGuid, "queryTest-" + RandomString.randomString(6));
        JSONArray array = new JSONArray(Arrays.asList(25));
        client.fieldReplaceOrCreateList(testEntry.getGuid(), fieldName, array, testEntry);
      }
    } catch (IOException | ClientException | NoSuchAlgorithmException e) {
      failWithStackTrace("Exception while tryint to create the guids: ", e);
    }

    try {
      String query = "~" + fieldName + " : ($gt: 0)";
      JSONArray result = client.selectQuery(query);
      for (int i = 0; i < result.length(); i++) {
        System.out.println(result.get(i).toString());
      }
      // best we can do should be at least 5, but possibly more objects in results
      Assert.assertThat(result.length(), greaterThanOrEqualTo(5));
    } catch (IOException | ClientException | JSONException e) {
      failWithStackTrace("Exception executing selectNear: ", e);
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
      JSONArray result = client.selectWithin(GNSProtocol.LOCATION_FIELD_NAME.toString(), rect);
      // best we can do should be at least 5, but possibly more objects in results
      Assert.assertThat(result.length(), greaterThanOrEqualTo(5));
    } catch (JSONException | IOException | ClientException e) {
      failWithStackTrace("Exception executing selectWithin: ", e);
    }
  }

  private static String createIndexTestField;

  /**
   *
   */
  @Test
  public void test_970_Http_CreateField() {
    createIndexTestField = "testField" + RandomString.randomString(6);
    try {
      client.fieldUpdate(masterGuid, createIndexTestField, createGeoJSONPolygon(AREA_EXTENT));
    } catch (JSONException | IOException | ClientException e) {
      failWithStackTrace("Exception during create field: ", e);
    }
  }

  /**
   *
   */
  @Test
  public void test_971_Http_CreateIndex() {
    try {
      client.fieldCreateIndex(masterGuid, createIndexTestField, "2dsphere");
    } catch (IOException | ClientException | JSONException e) {
      failWithStackTrace("Exception while creating index: ", e);
    }
  }

  /**
   *
   */
  @Test
  public void test_972_Http_SelectPass() {
    try {
      JSONArray result = client.selectQuery(buildQuery(createIndexTestField, AREA_EXTENT));
      for (int i = 0; i < result.length(); i++) {
        System.out.println(result.get(i).toString());
      }
      // best we can do should be at least 5, but possibly more objects in results
      Assert.assertThat(result.length(), greaterThanOrEqualTo(1));
    } catch (JSONException | IOException | ClientException e) {
      failWithStackTrace("Exception executing second selectNear: ", e);
    }
  }
  
   private static GuidEntry guidToDeleteEntry;
   private static GuidEntry mygroupEntry;
  /**
   *
   */
  @Test
  public void test_980_Http_GroupCreate() {
    String mygroupName = "mygroup" + RandomString.randomString(6);
    try {
      try {
        client.lookupGuid(mygroupName);
        failWithStackTrace(mygroupName + " entity should not exist");
      } catch (ClientException e) {
      }
      guidToDeleteEntry = client.guidCreate(masterGuid, "deleteMe" + RandomString.randomString(6));
      mygroupEntry = client.guidCreate(masterGuid, mygroupName);
    } catch (IOException | ClientException | NoSuchAlgorithmException e) {
      failWithStackTrace("Exception while creating guids: ", e);
    }
  }

  /**
   *
   */
  @Test
  public void test_981_Http_GroupAddHttpOne() {
    try {
      client.groupAddGuid(mygroupEntry.getGuid(), httpOneEntry.getGuid(), mygroupEntry);
    } catch (IOException | ClientException e) {
      failWithStackTrace("Exception while adding Westy: ", e);
    }
  }
  
  /**
   *
   */
  @Test
  public void test_982_GroupAddSam() {
    try {
      client.groupAddGuid(mygroupEntry.getGuid(), httpTwoEntry.getGuid(), mygroupEntry);
    } catch (IOException | ClientException e) {
      failWithStackTrace("Exception while adding Sam: ", e);
    }
  }

  /**
   *
   */
  @Test
  public void test_983_GroupAddGuidToDelete() {
    try {
      client.groupAddGuid(mygroupEntry.getGuid(), guidToDeleteEntry.getGuid(), mygroupEntry);
    } catch (IOException | ClientException e) {
      failWithStackTrace("Exception while adding GuidToDelete: ", e);
    }
  }

  /**
   *
   */
  @Test
  public void test_984_GroupAddCheck() {
    try {
      HashSet<String> expected = new HashSet<String>(Arrays.asList(httpOneEntry.getGuid(), httpTwoEntry.getGuid(), guidToDeleteEntry.getGuid()));
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(client.groupGetMembers(mygroupEntry.getGuid(), mygroupEntry));
      Assert.assertEquals(expected, actual);

      expected = new HashSet<String>(Arrays.asList(mygroupEntry.getGuid()));
      actual = JSONUtils.JSONArrayToHashSet(client.guidGetGroups(httpOneEntry.getGuid(), httpOneEntry));
      Assert.assertEquals(expected, actual);

    } catch (Exception e) {
      failWithStackTrace("Exception while getting members and groups: ", e);
    }
  }

  /**
   *
   */
  @Test
  public void test_985_GroupRemoveGuid() {
    // now remove a guid and check for group updates
    try {
      client.guidRemove(masterGuid, guidToDeleteEntry.getGuid());
    } catch (Exception e) {
      failWithStackTrace("Exception while removing testGuid: ", e);
    }
    try {
      client.lookupGuidRecord(guidToDeleteEntry.getGuid());
      failWithStackTrace("Lookup testGuid should have throw an exception.");
    } catch (ClientException e) {

    } catch (IOException e) {
      failWithStackTrace("Exception while doing Lookup testGuid: ", e);
    }
  }

  /**
   *
   */
  @Test
  public void test_986_GroupRemoveCheck() {
    try {
      HashSet<String> expected = new HashSet<String>(Arrays.asList(httpOneEntry.getGuid(), httpTwoEntry.getGuid()));
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(client.groupGetMembers(mygroupEntry.getGuid(), mygroupEntry));
      Assert.assertEquals(expected, actual);

    } catch (IOException | ClientException | JSONException e) {
      failWithStackTrace("Exception during remove guid group update test: ", e);
      System.exit(2);
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

  private static final void failWithStackTrace(String message, Exception... e) {
    if (e != null && e.length > 0) {
      e[0].printStackTrace();
    }
    org.junit.Assert.fail(message);
  }
}
