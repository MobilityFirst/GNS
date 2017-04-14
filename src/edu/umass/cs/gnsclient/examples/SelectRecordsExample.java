/* Copyright (c) 2016 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * Initial developer(s): Westy */
package edu.umass.cs.gnsclient.examples;

import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GNSCommand;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.ResponseCode;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;

import edu.umass.cs.gnscommon.utils.ThreadUtils;
import java.awt.geom.Point2D;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * This example creates an account GUID record, updates access lists,
 * performs a few reads and writes to its fields. It uses client
 * commands similar to those used by the CASA Alerts application.
 *
 * @author arun, westy
 */
public class SelectRecordsExample {

  // See http://geojson.org
  public static final String POLYGON = "Polygon";
  public static final String COORDINATES = "coordinates";
  public static final String TYPE = "type";
  // Some lat longs to use
  private static final double LEFT_LON = -98.08;
  private static final double TOP_LAT = 33.635;
  private static final double RIGHT_LON = -96.01;
  private static final double BOTTOM_LAT = 31.854;
  // Don't forget that longitude is X and latitude us Y so
  // for point systems it's often (long, lat).
  private static final Point2D DOMAIN_UPPER_LEFT = new Point2D.Double(LEFT_LON, TOP_LAT);
  private static final Point2D DOMAIN_UPPER_RIGHT = new Point2D.Double(RIGHT_LON, TOP_LAT);
  private static final Point2D DOMAIN_LOWER_RIGHT = new Point2D.Double(RIGHT_LON, BOTTOM_LAT);
  private static final Point2D DOMAIN_LOWER_LEFT = new Point2D.Double(LEFT_LON, BOTTOM_LAT);
  // Make a list of points that winds counter clockwise for GeoJSON (see notes below)
  private static final ArrayList<Point2D> AREA_EXTENT = new ArrayList<>(Arrays.asList(
          DOMAIN_UPPER_RIGHT, DOMAIN_UPPER_LEFT,
          DOMAIN_LOWER_LEFT, DOMAIN_LOWER_RIGHT,
          DOMAIN_UPPER_RIGHT));

  private static final String accountAlias = "exampleAccount@gns.name";
  private static GNSClient client;
  private static GuidEntry accountGuidEntry;

  /**
   * @param args
   * @throws IOException
   * @throws InvalidKeySpecException
   * @throws NoSuchAlgorithmException
   * @throws ClientException
   * @throws InvalidKeyException
   * @throws SignatureException
   * @throws Exception
   */
  public static void main(String[] args) throws IOException,
          InvalidKeySpecException, NoSuchAlgorithmException, ClientException,
          InvalidKeyException, SignatureException, Exception {

    // Create the client
    client = new GNSClient();
    // Create an account guid
    client.execute(GNSCommand.createAccount(accountAlias));
    accountGuidEntry = GuidUtils.getGUIDKeys(accountAlias);
    // Next we create some fake user records as subguids
    createUserRecords();
    // Display the query
    System.out.println(buildLocationsAndAgeQuery(GNSProtocol.LOCATION_FIELD_NAME.toString(), AREA_EXTENT,
            "age", 30, 50));
    // Execute the query and print the results
    System.out.println(client.execute(GNSCommand.selectRecords(accountGuidEntry,
            buildLocationsAndAgeQuery(GNSProtocol.LOCATION_FIELD_NAME.toString(), AREA_EXTENT,
                    "age", 40, 45),
            Arrays.asList(GNSProtocol.LOCATION_FIELD_NAME.toString(), "age"))).getResultJSONArray());
    ThreadUtils.sleep(30000);
    System.exit(0);
  }

  private static void createUserRecords() {
    Random random = new Random();

    for (int i = 0; i < 30; i++) {
      try {
        client.execute(GNSCommand.createGUID(accountGuidEntry, "user-" + i));
        GuidEntry userGuid = GuidUtils.getGUIDKeys("user-" + i);
        System.out.print("+");
        double lon = LEFT_LON + (RIGHT_LON - LEFT_LON) * random.nextDouble();
        double lat = BOTTOM_LAT + (TOP_LAT - BOTTOM_LAT) * random.nextDouble();
        client.execute(GNSCommand.setLocation(userGuid, lon, lat));
        client.execute(GNSCommand.fieldUpdate(userGuid, "age", random.nextInt(40) + 20));
      } catch (ClientException e) {
        if (ResponseCode.CONFLICTING_GUID_EXCEPTION.equals(e.getCode())) {
          System.out.print(".");
        } else {
          System.out.println("Problem creating user: " + e.getMessage());
        }
      } catch (IOException e) {
        System.out.println("Problem creating user: " + e.getMessage());
      }
    }
    System.out.println();
  }

  /**
   * Creates a query that checks for records with an ageField between age1 and age2
   * and overlaps between a polygon
   * specified as a closed list of polygons and a the location
   * field in a GUID record.
   *
   * @param locationField
   * @param coordinates
   * @param ageField
   * @param age1
   * @param age2
   * @return
   * @throws JSONException
   */
  private static String buildLocationsAndAgeQuery(String locationField, List<Point2D> coordinates,
          String ageField, int age1, int age2) throws JSONException {
    return "$and: ["
            + "{"
            + buildLocationsQuery(locationField, coordinates)
            //            + "{~" + locationField + ":{"
            //            + "$geoIntersects:{"
            //            //+ "$geoWithin:{"
            //            + "$geometry:"
            //            + createGeoJSONPolygon(coordinates).toString()
            //            + "}}},"
            + "},"
            + "{~" + ageField + ":{$gt:" + age1 + ", $lt:" + age2 + "}}"
            + "]";
  }

  /**
   * Creates a query that checks for overlaps between a polygon
   * specified as a closed list of polygons and a the location
   * field in a GUID record.
   *
   * @param locationField
   * @param coordinates
   * @return
   * @throws JSONException
   */
  // Mongo-like query syntax:
  //  "{\"~<locationField>\": {"
  //          + "      $geoIntersects: {"
  //          + "         $geometry: {"
  //          + "            type: \"Polygon\","
  //          + "            coordinates: [ <coordinates> ]\n"
  //          + "         }"
  //          + "      }"
  //          + "   }"
  //          + "}";
  private static String buildLocationsQuery(String locationField, List<Point2D> coordinates) throws JSONException {
    return "~" + locationField + ":{"
            + "$geoIntersects:{"
            + "$geometry:"
            + createGeoJSONPolygon(coordinates).toString()
            + "}"
            + "}";
  }

  /**
   * Creates a GeoJSON polygon from a list of coordinates.
   * Note that GeoJSON polygons should use counter clockwise
   * winding and that the polygon needs to be closed (first and
   * last points the same).
   *
   * @param coordinates
   * @return
   * @throws JSONException
   */
  private static JSONObject createGeoJSONPolygon(List<Point2D> coordinates) throws JSONException {
    JSONObject json = new JSONObject();
    json.put(TYPE, POLYGON);
    json.put(COORDINATES, createGeoJSONPolygonArray(coordinates));
    return json;
  }

  //Note that GeoJSON polygons should use a counter clockwise
  //winding and that the polygon needs to be closed (first and
  //last points the same).
  private static JSONArray createGeoJSONPolygonArray(List<Point2D> coordinates) throws JSONException {
    JSONArray ring = new JSONArray();
    for (int i = 0; i < coordinates.size(); i++) {
      ring.put(i, new JSONArray(
              Arrays.asList(coordinates.get(i).getX(), coordinates.get(i).getY())));
    }
    // From http://geojson.org
    // Coordinates of a Polygon are an array of LinearRing coordinate arrays. 
    // The first element in the array represents the exterior ring. 
    // Any subsequent elements represent interior rings (or holes).
    JSONArray jsonCoordinates = new JSONArray();
    jsonCoordinates.put(0, ring);
    return jsonCoordinates;
  }
}
