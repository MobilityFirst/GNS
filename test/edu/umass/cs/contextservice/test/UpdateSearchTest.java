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
 *  Initial developer(s): 
 *
 */
package edu.umass.cs.contextservice.test;

import edu.umass.cs.acs.geodesy.GlobalCoordinate;
import edu.umass.cs.contextservice.client.ContextServiceClient;
import edu.umass.cs.gnsclient.client.GuidEntry;
import edu.umass.cs.gnsclient.client.UniversalTcpClient;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnsclient.client.util.ServerSelectDialog;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.json.JSONException;
import org.json.JSONObject;

import static org.junit.Assert.*;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Vector;

import org.json.JSONArray;

/**
 * JSON User update test for the GNS.
 *
 */

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class UpdateSearchTest {

  private static final String ACCOUNT_ALIAS = "support@gns.name"; // REPLACE THIS WITH YOUR ACCOUNT ALIAS
  private static final String PASSWORD = "password";
  private static UniversalTcpClient client;
  /**
   * The address of the GNS server we will contact
   */
  private static InetSocketAddress address = null;
  private static GuidEntry masterGuid;
  //private static GuidEntry westyEntry;
  //private static GuidEntry samEntry;
  private static ContextServiceClient<String> contextServiceClient = null;
  
  private static final int NUM_GUIDs	= 10;
  

  
  public UpdateSearchTest() {
    if (address == null) 
    {
      address = ServerSelectDialog.selectServer();
      try 
      {
		contextServiceClient = new ContextServiceClient<String>("127.0.0.1", 5000);
      } catch (IOException e1) 
      {
		e1.printStackTrace();
      }
      //client = new BasicUniversalTcpClient(address.getHostName(), address.getPort(), true);
      client = new UniversalTcpClient(address.getHostName(), address.getPort(), true);
      try 
      {
        masterGuid = GuidUtils.lookupOrCreateAccountGuid(client, ACCOUNT_ALIAS, PASSWORD, true);
      } catch (Exception e) 
      {
        fail("Exception when we were not expecting it: " + e);
      }
    }
  }

  @Test
  public void test_1_Updates() 
  {
	  Random rand = new Random();
	  Vector<String> guidStorage = new Vector<String>();
	  
	  for( int i=0;i<NUM_GUIDs;i++ )
	  {
		  try 
		  {
			  GuidEntry currGuidEntry = 
					  GuidUtils.registerGuidWithTestTag(client, masterGuid, "testGUID1" + i);
			  
			  guidStorage.add(currGuidEntry.getGuid());
			  
			  JSONObject attrValuePair = new JSONObject();
			  
			  double randomLat = (rand.nextDouble()*180)-90;
			  double randomLong = (rand.nextDouble()*360)-180;
			  
			  
			  randomLat = 42.466;
			  randomLong = -72.58;
			  
			  attrValuePair.put("geoLocationCurrentLat", randomLat);
			  attrValuePair.put("geoLocationCurrentLong", randomLong);
			  
			  // everything is sent as a string to GNS
			  client.fieldCreateList(currGuidEntry.getGuid(), "geoLocationCurrentLat", new JSONArray().put(randomLat+""), currGuidEntry);
			  //fieldUpdate(currGuidEntry, "latitude", randomLat);
			  client.fieldCreateList(currGuidEntry.getGuid(), "geoLocationCurrentLong", new JSONArray().put(randomLong+""), currGuidEntry);
			  
			  System.out.println("Updates sent for "+i);
		  } catch (Exception e)
		  {
			  e.printStackTrace();
			  fail("Exception while updating JSON: " + e);
		  }
		  catch(Error e)
		  {
			  e.printStackTrace();
			  fail("Error while updating JSON: " + e);
		  }
	  }
	  // wait till all GUIDs are stored as GNS updates are non blocking for context service. 
	  try 
	  {
		  Thread.sleep(5000);
	  } catch (InterruptedException e1) 
	  {
		  e1.printStackTrace();
	  }
	  // do gets to verify
	  
	  for(int i=0;i<guidStorage.size();i++)
	  {
		  String currGUID = guidStorage.get(i);
		  JSONObject recvJSON = contextServiceClient.sendGetRequest(currGUID);
		  assert(recvJSON.length()>0);
		  System.out.println("i "+i+" GUID "+currGUID+" JSON "+recvJSON);
		  
	  }
	  
	  // issue different types of search
	  // simple search to get all the records
	  String query = "SELECT GUID_TABLE.guid FROM GUID_TABLE WHERE geoLocationCurrentLat >= -90 AND geoLocationCurrentLong <= 180";
	  //JSONObject geoJSONObject = getGeoJSON();
	  JSONArray recvArray = contextServiceClient.sendSearchQuery(query);
	  
	  int numGuidsRet = 0;
	  System.out.println("Search query result length "+recvArray.length());
	  // JSONArray is an array of GUIDs
	  HashMap<String, Boolean> guidsRet = new HashMap<String, Boolean>();
	  
	  for(int i=0; i<recvArray.length(); i++)
	  {
		  try
		  {
			  guidsRet.put(recvArray.getString(i), true);
			  System.out.println("GUIDs returned "+recvArray.getString(i));
		  } catch (JSONException e)
		  {
			  e.printStackTrace();
		  }
	  }
	  numGuidsRet = guidsRet.size();
	  System.out.println("Search query num GUIDs "+numGuidsRet);
	  // all guids should be there
	  for(int i=0;i<guidStorage.size();i++)
	  {
		  String currGUID = guidStorage.get(i);
		  if(!guidsRet.containsKey(currGUID))
		  {
			  System.out.println("GUID not there "+currGUID);
			  assert(false);
		  }
	  }
	  
	  // simple search to get all the records
	  JSONObject geoJSONObject = getGeoJSON();
	  System.out.println("GeoJSON created "+geoJSONObject);
	  query = "SELECT GUID_TABLE.guid FROM GUID_TABLE WHERE GeojsonOverlap(geoLocationCurrentLat, "
	  		+ "geoLocationCurrentLong, "+geoJSONObject.toString()+")";
	  recvArray = contextServiceClient.sendSearchQuery(query);
	  
	  System.out.println("Num guids returned by geoJSON query "+recvArray.length());
  }
  
  private JSONObject getGeoJSON()
  {
	  List<GlobalCoordinate> list = new LinkedList<GlobalCoordinate>();
	  GlobalCoordinate amherst = new GlobalCoordinate(42.340382, -72.496819);
		GlobalCoordinate northampton = new GlobalCoordinate(42.3250896, -72.6412013);
		GlobalCoordinate sunderland = new GlobalCoordinate(42.4663727, -72.5795115);
		list.add(amherst);
		list.add(sunderland);
		list.add(northampton);
		list.add(amherst);
		JSONObject geoJSON = null;
		try 
		{
			 geoJSON = GeoJSON.createGeoJSONPolygon(list);
			 /*JSONArray coordArray = geoJSON.getJSONArray("coordinates");
			 JSONArray newArray = new JSONArray(coordArray.getString(0));
			 for(int i=0;i<newArray.length();i++)
			 {
				 JSONArray coordList = new JSONArray(newArray.getString(i));
				 ContextServiceLogger.getLogger().fine("i "+i+coordList.getDouble(0) );
			 }*/
		} catch (JSONException e)
		{
			e.printStackTrace();
		}
		return geoJSON;
  }
}