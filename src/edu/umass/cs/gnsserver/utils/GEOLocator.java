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
package edu.umass.cs.gnsserver.utils;

import edu.umass.cs.gnsserver.httpserver.Defs;
import edu.umass.cs.gnsserver.main.GNS;
import java.awt.geom.Point2D;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class GEOLocator {

  //private final static String queryURL = "http://api.ipinfodb.com/v3/ip-city/?key=7ff99f3ea43b4f01c4947512822e4d660bf5832a6777f484ed5b964729a50b2b&format=json";
  private final static String IPServiceURL = "http://www.geoplugin.net/json.gp";
  /*
   {
   "geoplugin_request":"54.248.152.94",
   "geoplugin_status":200,
   "geoplugin_city":"Tokyo",
   "geoplugin_region":"T\u014dky\u014d",
   "geoplugin_areaCode":"0",
   "geoplugin_dmaCode":"0",
   "geoplugin_countryCode":"JP",
   "geoplugin_countryName":"Japan",
   "geoplugin_continentCode":"AS",
   "geoplugin_latitude":"35.685001373291",
   "geoplugin_longitude":"139.75140380859",
   "geoplugin_regionCode":"40",
   "geoplugin_regionName":"T\u014dky\u014d",
   "geoplugin_currencyCode":"JPY",
   "geoplugin_currencySymbol":"&#165;",
   "geoplugin_currencyConverter":79.8354
   }
   */

  /**
   * Lookup the lat,long based on the ip address.
   * 
   * @param ipAddress
   * @return a Point2D
   */
  public static Point2D lookupIPLocation(String ipAddress) {
    String query = IPServiceURL + Defs.QUERYPREFIX + "ip" + Defs.VALSEP + ipAddress;
    JSONObject json = sendGetCommandReadJSONObject(query);
    try {
      if (json != null 
              && (json.getInt("geoplugin_status") == 200 
              // partial content status
              || json.getInt("geoplugin_status") == 206)) {
        return new Point2D.Double(Double.parseDouble(json.getString("geoplugin_longitude")), Double.parseDouble(json.getString("geoplugin_latitude")));
      }
    } catch (JSONException e) {
      GNS.getLogger().severe("Unable to parse JSON: " + e.getMessage());
    }
    return null;
  }
  
  private final static String cityServiceURL = "http://maps.googleapis.com/maps/api/geocode/json";
  
  //  http://maps.googleapis.com/maps/api/geocode/json?address=Palo+Alto,+CA&sensor=false
  
  /**
   * Lookup the lat,long based on the street address.
   * 
   * @param address
   * @return a Point2D
   */
    
  public static Point2D lookupCityLocation(String address) {
    String query = cityServiceURL + Defs.QUERYPREFIX + "address" + Defs.VALSEP 
            + address.replace(" ", "+") + Defs.KEYSEP + "sensor=false";
    JSONObject json = sendGetCommandReadJSONObject(query);
    try {
      //System.out.println(json.getString("status"));
      if (json != null && ("OK".equals(json.getString("status")))) {
        JSONArray results = json.getJSONArray("results");
        //System.out.println("results=" + results);
        JSONObject result = results.getJSONObject(0);
        //System.out.println("result=" + result);
        JSONObject location = result.getJSONObject("geometry").getJSONObject("location");
        System.out.println("address = " + address + " location=" + location);
        return new Point2D.Double(location.getDouble("lng"), location.getDouble("lat"));
      }
    } catch (JSONException e) {
      GNS.getLogger().severe("Unable to parse JSON: " + e.getMessage());
    }
    return null;
  }

  private static JSONObject sendGetCommandReadJSONObject(String urlString) {
    HttpURLConnection connection = null;
    try {
      //System.out.println(urlString);
      URL serverAddress = new URL(urlString);
      //Set up the initial connection
      connection = (HttpURLConnection) serverAddress.openConnection();
      connection.setRequestMethod("GET");
      connection.setDoOutput(true);
      connection.setReadTimeout(10000);

      connection.connect();
      String line;
      int cnt = 3;
      do {
        try {
          BufferedReader rd = new BufferedReader(new InputStreamReader(connection.getInputStream()));
          StringBuilder sb = new StringBuilder();

          while ((line = rd.readLine()) != null) {
            //System.out.println(line);
            sb.append(line + '\n');
          }
          return new JSONObject(sb.toString());

        } catch (java.net.SocketTimeoutException e) {
          GNS.getLogger().info("Get Response timed out. Trying " + cnt + " more times.");
        } catch (JSONException e) {
          GNS.getLogger().severe("Unable to parse JSON: " + e.getMessage());
          return null;
        }
      } while (cnt-- > 0);
      return null;
    } catch (MalformedURLException e) {
      e.printStackTrace();
    } catch (ProtocolException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      //close the connection, set all objects to null
      if (connection != null) {
        connection.disconnect();
      }
    }
    return null;
  }
  
  /**
   *
   * @param args
   */
  public static void main(String[] args) {
    System.out.println(lookupIPLocation("74.125.45.100"));
    System.out.println(lookupIPLocation("50.138.213.8"));
    System.out.println(lookupIPLocation("54.248.152.94"));
    System.out.println(lookupCityLocation("Palo Alto, USA"));
    System.out.println(lookupCityLocation("Sal Palo, Brazil"));
  }
}
