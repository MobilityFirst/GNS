package edu.umass.cs.gnsclient.client.singletests;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.umass.cs.gnsserver.gnsapp.packet.CommandPacket;
import edu.umass.cs.utils.Util;

public class ByteificationComparison {

	private static JSONObject testJson;
	
	@BeforeClass
	public static void setupBeforClass() throws JSONException{

		Collection<String> collection1 = new ArrayList<String>();
		Collection<String> collection2 = new ArrayList<String>();
		Collection<String> collection3 = new ArrayList<String>();
		Collection<Collection> recursiveCollection = new ArrayList<Collection>();
		
		for (int i = 0; i < 5; i++){
			collection1.add(new String(Util.getRandomAlphanumericBytes(64)));
		}
		for (int i = 0; i < 3; i++){
			collection2.add(new String(Util.getRandomAlphanumericBytes(64)));
		}
		for (int i = 0; i < 7; i++){
			collection3.add(new String(Util.getRandomAlphanumericBytes(32)));
		}
		recursiveCollection.add(collection1);
		recursiveCollection.add(collection2);
		testJson = new JSONObject();
		testJson.put("collection", recursiveCollection);
		testJson.put("otherCollection", collection3);
		
	}
	public byte[] recursiveCollectionToBytes(JSONArray c) throws JSONException, IOException{
		final short STRING_IDENTIFIER = 1;
		final short COLLECTION_IDENTIFIER = 2;
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		int len1 = c.length();
		out.write(COLLECTION_IDENTIFIER);
		out.write(len1);
		for (int i = 0; i < len1; i++){
			Object obj = c.get(i);
			if ( obj instanceof JSONArray){
				out.write(recursiveCollectionToBytes((JSONArray)obj));
			}
			else{
				int len2 = ((String)obj).length();
				out.write(STRING_IDENTIFIER);
				out.write(len2);
				out.write(((String)obj).getBytes());
			}

		}
		return out.toByteArray();
		
	}
	
	public byte[] recursiveToBytes(JSONObject json) throws JSONException, IOException{
		final short STRING_IDENTIFIER = 1;
		Iterator<String> keys = json.keys();
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		while (keys.hasNext()){
			String key = keys.next();
			Object obj = json.get(key);
			int len1 = key.length();
			out.write(STRING_IDENTIFIER);
			out.write(len1);
			out.write(key.getBytes());
			if ( obj instanceof JSONArray){
				out.write(recursiveCollectionToBytes((JSONArray)obj));
			}
			else{
				int len2 = ((String)obj).length();
				out.write(STRING_IDENTIFIER);
				out.write(len2);
				out.write(((String)obj).getBytes());
			}

			}
		return out.toByteArray();
	}
	
	private final int TEST_RUNS = 1000000;
	@Test
	public void test_01_JSON_Default() throws JSONException {
		long startTime = System.nanoTime();
		for (int i = 0; i < TEST_RUNS; i++){
			testJson.toString().getBytes();
		}
		long endTime = System.nanoTime();
		double avg = (endTime - startTime) / (TEST_RUNS);
		System.out.println("Average byteification time JSON was " + avg + " nanoseconds.");
		
	}
	
	@Test
	public void test_02_recursive() throws JSONException, IOException {
		long startTime = System.nanoTime();
		for (int i = 0; i < TEST_RUNS; i++){
			recursiveToBytes(testJson);
		}
		long endTime = System.nanoTime();
		double avg = (endTime - startTime) / (TEST_RUNS);
		System.out.println("Average byteification time RECURSIVE was " + avg + " nanoseconds.");
		
	}

}
