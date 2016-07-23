package edu.umass.cs.gnsclient.benchmarking;

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
 * Initial developer(s): aditya */

import edu.umass.cs.gnsclient.client.GuidEntry;
import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnscommon.GNSCommandProtocol;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.json.JSONArray;
import org.json.JSONObject;

/**
 * This example creates an account GUID record, performs a few reads and writes
 * to its fields, and deletes the record.
 * <p>
 * Note: This example assumes that the verification step (e.g., via email) to
 * verify an account GUID's human-readable name has been disabled on the server
 * using the -disableEmailVerification option.
 * 
 * @author aditya
 */
public class SelectCallBenchmarking
{
	// after sending all the requests it waits for 100 seconds 
	public static final int WAIT_TIME							= 100000;
	// 1% loss tolerance
	public static final double INSERT_LOSS_TOLERANCE			= 0.0;
	
	// 1% loss tolerance
	public static final double UPD_LOSS_TOLERANCE				= 0.0;
	
	// 1% loss tolerance
	public static final double SEARCH_LOSS_TOLERANCE			= 0.0;
	
	public static final String ACCOUNT_ALIAS 					= "admin@gns.name";
	
	
	// dallas region in texas area, for which we have weather alerts.
	public static final double LONGITUDE_MIN 					= -98.08;
	public static final double LONGITUDE_MAX 					= -96.01;
	
	public static final double LATITUDE_MAX 					= 33.635;
	public static final double LATITUDE_MIN 					= 31.854;
	
	public static final int NUM_GUIDs							= 100;
	
	public static final int NUM_SELECT_OPER						= 1000;
	
	public static final String GUID_PREFIX 						= "GUIDPref";
	
	public static final String Latitude_Name					= "latitude";
	public static final String Longitude_Name					= "longitude";
	
	public static final int THREAD_POOL_SIZE					= 1;
	public static Random randomGen								= new Random();
	
	// replace with your account alias
	public static GNSClientCommands client;
	public static GuidEntry account_guid;
	//private static List<GuidEntry>
		
	public static ExecutorService	 taskES						= null;
	
	public static final double LEFT 							= LONGITUDE_MIN;
	public static final double RIGHT 							= LONGITUDE_MAX;
	public static final double TOP 								= LATITUDE_MAX;
	public static final double BOTTOM 							= LATITUDE_MIN;
	
	public static JSONArray UPPER_LEFT ;
	//	= new GlobalCoordinate(TOP, LEFT);
	
	public static JSONArray UPPER_RIGHT;
	//= new GlobalCoordinate(TOP, RIGHT);
	
	public static JSONArray LOWER_RIGHT;
	//= new GlobalCoordinate(BOTTOM, RIGHT);
	
	public static JSONArray LOWER_LEFT;
	//= new GlobalCoordinate(BOTTOM, LEFT);
	
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
			InvalidKeyException, SignatureException, Exception 
	{
		taskES =  Executors.newFixedThreadPool(THREAD_POOL_SIZE);
		
		UPPER_LEFT = new JSONArray("["+LEFT+","+TOP+"]");
//		= new GlobalCoordinate(TOP, LEFT);
		
		UPPER_RIGHT = new JSONArray("["+RIGHT+","+TOP+"]");
		//= new GlobalCoordinate(TOP, RIGHT);
		
		LOWER_RIGHT = new JSONArray("["+RIGHT+","+BOTTOM+"]");
		//= new GlobalCoordinate(BOTTOM, RIGHT);
		
		LOWER_LEFT = new JSONArray("["+LEFT+","+BOTTOM+"]");
		//= new GlobalCoordinate(BOTTOM, LEFT);
		
		/* Create the client that connects to a default reconfigurator as
		 * specified in gigapaxos properties file. */
		client = new GNSClientCommands();
		System.out.println("[Client connected to GNS]\n");	
		
		try
		{
			/**
			 * Create an account GUID if one doesn't already exists. The true
			 * flag makes it verbosely print out what it is doing. The password
			 * is for future use and is needed mainly if the keypair is
			 * generated on the server in order to retrieve the private key.
			 * lookupOrCreateAccountGuid "cheats" by bypassing email-based or
			 * other verification mechanisms using a shared secret between the
			 * server and the client.
			 * */
			System.out
					.println("// account GUID creation\n"
							+ "GuidUtils.lookupOrCreateAccountGuid(client, ACCOUNT_ALIAS,"
							+ " \"password\", true)");
			account_guid = GuidUtils.lookupOrCreateAccountGuid(client, ACCOUNT_ALIAS,
					"password", true);
		} catch (Exception | Error e) 
		{
			System.out.println("Exception during accountGuid creation: " + e);
			e.printStackTrace();
			System.exit(1);
		}
		
		// Create a JSON Object to initialize our guid record
		JSONObject json = new JSONObject("{\"occupation\":\"busboy\","
				+ "\"friends\":[\"Joe\",\"Sam\",\"Billy\"],"
				+ "\"gibberish\":{\"meiny\":\"bloop\",\"einy\":\"floop\"},"
				+ "\"location\":\"work\",\"name\":\"frank\"}");

		// Write out the JSON Object
		client.update(account_guid, json);
		System.out.println("\n// record update\n"
				+ "client.update(GUID, record) // record=" + json);

		// and read the entire object back in
		JSONObject result = client.read(account_guid);
		System.out.println("client.read(GUID) -> " + result.toString());

		// Change a field
		client.update(account_guid, new JSONObject(
				"{\"occupation\":\"rocket scientist\"}"));
		System.out
				.println("\n// field update\n"
						+ "client.update(GUID, fieldKeyValue) // fieldKeyValue={\"occupation\":\"rocket scientist\"}");

		// and read the entire object back in
		result = client.read(account_guid);
		System.out.println("client.read(GUID) -> " + result.toString());

		// Add a field
		client.update(account_guid, new JSONObject("{\"ip address\":\"127.0.0.1\"}"));
		System.out
				.println("\n// field add\n"
						+ "client.update(GUID, fieldKeyValue) // fieldKeyValue= {\"ip address\":\"127.0.0.1\"}");
		
		// and read the entire object back in
		result = client.read(account_guid);
		System.out.println("client.read(GUID) -> " + result.toString());

		// Remove a field
		client.fieldRemove(account_guid.getGuid(), "gibberish", account_guid);
		System.out.println("\n// field remove\n"
				+ "client.fieldRemove(GUID, \"gibberish\")");

		// and read the entire object back in
		result = client.read(account_guid);
		System.out.println("client.read(GUID) -> " + result.toString());

		// Add some more stuff to read back
		JSONObject newJson = new JSONObject();
		JSONObject subJson = new JSONObject();
		subJson.put("sally", "red");
		subJson.put("sammy", "green");
		JSONObject subsubJson = new JSONObject();
		subsubJson.put("right", "seven");
		subsubJson.put("left", "eight");
		subJson.put("sally", subsubJson);
		newJson.put("flapjack", subJson);
		client.update(account_guid, newJson);
		System.out.println("\n// field add with JSON value\n"
				+ "client.update(GUID, fieldKeyValue) // fieldKeyValue="
				+ newJson);
		
		// Read a single field at the top level
		String resultString = client.fieldRead(account_guid, "flapjack");
		System.out.println("client.fieldRead(\"flapjack\") -> " + resultString);
		
		// Read a single field using dot notation
		resultString = client.fieldRead(account_guid, "flapjack.sally.right");
		System.out.println("\n// dotted field read\n"
				+ "client.fieldRead(GUID, \"flapjack.sally.right\") -> "
				+ resultString);
		
		// Update a field using dot notation
		JSONArray newValue = new JSONArray(
				Arrays.asList("One", "Ready", "Frap"));
		client.fieldUpdate(account_guid, "flapjack.sammy", newValue);
		System.out.println("\n// dotted field update\n"
				+ "client.fieldUpdate(GUID, \"flapjack.sammy\", " + newValue);
		
		// Read the same field using dot notation
		resultString = client.fieldRead(account_guid, "flapjack.sammy");
		System.out.println("client.fieldRead(GUID, \"flapjack.sammy\") -> "
				+ resultString);
		
		// Read two fields at a time
		resultString = client.fieldRead(account_guid,
				new ArrayList<String>(Arrays.asList("name", "occupation")));
		System.out.println("\n// multi-field read\n"
				+ "client.fieldRead(GUID, [\"name\",\"occupation\"] -> "
				+ resultString);
		
		// Read the entire object back in
		result = client.read(account_guid);
		System.out.println("\nclient.read(GUID) -> " + result.toString());
		
		
		//insertGUIDs();
		AbstractRequestSendingClass requestTypeObj = null;
		requestTypeObj = new InsertClass();
		
		new Thread(requestTypeObj).start();
		requestTypeObj.waitForThreadFinish();
		
		issueWholeRegionSelectQuery();
		//client.select
		// Delete created GUID
		client.accountGuidRemove(account_guid);
		System.out.println("\n// GUID delete\n"
				+ "client.accountGuidRemove(GUID) // GUID=" + account_guid);
		
		// Try read the entire record
		try
		{
			result = client.read(account_guid);
		} catch (Exception e) {
			System.out.println("\n// non-existent GUID error (expected)\n"
					+ "client.read(GUID) // GUID= " + account_guid + "\n  "
					+ e.getMessage());
		}
		
		client.close();
		System.out.println("\nclient.close() // test successful");
	}
	
	public static void issueWholeRegionSelectQuery()
	{
		JSONArray wholeRegion = new JSONArray();
		wholeRegion.put(UPPER_LEFT);
		wholeRegion.put(LOWER_RIGHT);
		
		// a test select
		try
		{
			JSONArray resultArray = client.selectWithin
						(GNSCommandProtocol.LOCATION_FIELD_NAME , wholeRegion);
			System.out.println("Total guids returned "+resultArray.length());
			assert( resultArray.length() == NUM_GUIDs );
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public static void insertGUIDs()
	{
		for( int i=0; i<NUM_GUIDs; i++ )
		{
			String guidAlias = GUID_PREFIX + i;
			
			try
			{
				double randLat = LATITUDE_MIN + randomGen.nextDouble()*(LATITUDE_MAX - LATITUDE_MIN);
				double randLong = LONGITUDE_MIN + randomGen.nextDouble()*(LONGITUDE_MAX - LONGITUDE_MIN);
				JSONObject updateJSON = new JSONObject();
				updateJSON.put(Latitude_Name, randLat);
				updateJSON.put(Longitude_Name, randLong);
				
				//JSONArray array = new JSONArray(Arrays.asList(randLong, randLat));
				System.out.println("Creating GUID alias "+guidAlias);
				GuidEntry guidEntry = client.guidCreate(account_guid, guidAlias);
				client.setLocation(guidEntry, randLong, randLat);
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
		}
		
		JSONArray wholeRegion = new JSONArray();
		wholeRegion.put(UPPER_LEFT);
		wholeRegion.put(LOWER_RIGHT);
		
		// a test select
		try
		{
			JSONArray resultArray = client.selectWithin
						(GNSCommandProtocol.LOCATION_FIELD_NAME , wholeRegion);
			System.out.println("Total guids returned "+resultArray.length());
			assert(resultArray.length() == NUM_GUIDs);	
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public static void perfromSelectOperations()
	{
//		for( int i=0; i<NUM_SELECT_OPER; i++ )
//		{
//			double randLat1 = LATITUDE_MIN + randomGen.nextDouble()*(LATITUDE_MAX - LATITUDE_MIN);
//			double randLong1 = LONGITUDE_MIN + randomGen.nextDouble()*(LONGITUDE_MAX - LONGITUDE_MIN);
//			
//			double randLat2 = LATITUDE_MIN + randomGen.nextDouble()*(LATITUDE_MAX - LATITUDE_MIN);
//			double randLong2 = LONGITUDE_MIN + randomGen.nextDouble()*(LONGITUDE_MAX - LONGITUDE_MIN);
//			
//			double minLat = (randLat1>randLat2)?randLat2:randLat1;
//			double maxLat = (randLat1>randLat2)?randLat1:randLat2;
//			
//			JSONArray minPoint = new JSONArray();
//			
//			double minLong = (randLong1>randLong2)?randLong2:randLong1;
//			double maxLong = (randLong1>randLong2)?randLong1:randLong2;
//			
//			client.selectWithin(GNSCommandProtocol.LOCATION_FIELD_NAME , value)
//			
//		}
	}
}