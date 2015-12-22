package edu.umass.cs.contextservice.test;

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
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.acs.geodesy.GlobalCoordinate;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.contextservice.utils.Utils;

/**
*
* @author westy
*/
public class GeoJSON 
{

 public static JSONObject createGeoJSONPolygon(List<GlobalCoordinate> coordinates) throws JSONException {
   JSONArray ring = new JSONArray();
   for (int i = 0; i < coordinates.size(); i++) {
     ring.put(i, new JSONArray(
             Arrays.asList(roundCoordinate(coordinates.get(i).getLong()),
                     roundCoordinate(coordinates.get(i).getLat()))));
   }
   JSONArray jsonCoordinates = new JSONArray();
   jsonCoordinates.put(0, ring);
   JSONObject json = new JSONObject();
   json.put("type", "Polygon");
   json.put("coordinates", jsonCoordinates);
   return json;
 }

 //'{"type": "Point", "coordinates": [-100, 80]}'
 public static JSONObject createGeoJSONCoordinate(GlobalCoordinate coordinate) throws JSONException {
   JSONObject json = new JSONObject();
   json.put("type", "Point");
   json.put("coordinates", new JSONArray(Arrays.asList(roundCoordinate(coordinate.getLong()),
           roundCoordinate(coordinate.getLat()))));
   return json;
 }

 public static GlobalCoordinate readGeoJSONCoordinate(JSONObject json) throws JSONException {
   assert json.optString("type").equals("Point");
   JSONArray coordinate = json.getJSONArray("coordinates");
   return new GlobalCoordinate(roundCoordinate(coordinate.getDouble(1)),
           roundCoordinate(coordinate.getDouble(0)));
 }
 
 public final static int COORDINATE_PRECISION = 3;
 
 private static double roundCoordinate(double coord) {
   return Utils.roundTo(coord, COORDINATE_PRECISION);
 }
 
 
 public static void main(String[] args)
 {
	 List<GlobalCoordinate> list = new LinkedList<GlobalCoordinate>();
	 GlobalCoordinate amherst = new GlobalCoordinate(42.340382, -72.496819);
	 GlobalCoordinate northampton = new GlobalCoordinate(42.3250896, -72.6412013);
	 GlobalCoordinate sunderland = new GlobalCoordinate(42.4663727, -72.5795115);
	 list.add(amherst);
	 list.add(sunderland);
	 list.add(northampton);
	 list.add(amherst);
	 try 
	 {
		 JSONObject geoJSON = createGeoJSONPolygon(list);
		 JSONArray coordArray = geoJSON.getJSONArray("coordinates");
		 JSONArray newArray = new JSONArray(coordArray.getString(0));
		 for(int i=0;i<newArray.length();i++)
		 {
			 JSONArray coordList = new JSONArray(newArray.getString(i));
			 ContextServiceLogger.getLogger().fine("i "+i+coordList.getDouble(0) );
		 }
	 } catch (JSONException e) 
	 {
		e.printStackTrace();
	 }
 }
 
}