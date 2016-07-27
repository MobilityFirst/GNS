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

import com.fasterxml.jackson.core.JsonProcessingException;

import edu.umass.cs.gnscommon.utils.JSONByteConverter;
import edu.umass.cs.gnsserver.gnsapp.packet.CommandPacket;
import edu.umass.cs.utils.Util;

public class ByteificationComparison {

	private static JSONObject testJson;
	private final int TEST_RUNS = 1000000;
	private final short STRING_IDENTIFIER = 1;
	private final short COLLECTION_IDENTIFIER = 2;
	
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
	@Test
	public void test_01_JSON_Default() throws JSONException {
		long startTime = System.nanoTime();
		for (int i = 0; i < TEST_RUNS; i++){
			byte[] bytes = testJson.toString().getBytes();
			JSONObject testJson2 = new JSONObject(new String(bytes));
			//assert(testJson.equals(testJson2));
		}
		long endTime = System.nanoTime();
		double avg = (endTime - startTime) / (TEST_RUNS);
		System.out.println("Average byteification time JSON was " + avg + " nanoseconds.");
		
		byte[] bytes = testJson.toString().getBytes();
		JSONObject testJson2 = new JSONObject(new String(bytes));
		assert(testJson.toString().equals(testJson2.toString()));
		//System.out.println("JSON1: \n" + testJson.toString());
		//System.out.println("JSON2: \n" + testJson2.toString());
		
	}
	
	@Test
	public void test_02_hardcoded() throws JSONException, IOException {
		long startTime = System.nanoTime();
		for (int i = 0; i < TEST_RUNS; i++){
			byte[] bytes = JSONByteConverter.toBytesHardcoded(testJson);
			JSONObject testJson2 = JSONByteConverter.fromBytesHardcoded(bytes);
			//assert(testJson.equals(testJson2));
		}
		long endTime = System.nanoTime();
		double avg = (endTime - startTime) / (TEST_RUNS);
		System.out.println("Average byteification time HARDCODED was " + avg + " nanoseconds.");
		
		byte[] bytes = JSONByteConverter.toBytesHardcoded(testJson);
		JSONObject testJson2 = JSONByteConverter.fromBytesHardcoded(bytes);
		assert(testJson.toString().equals(testJson2.toString()));
		//System.out.println("JSON1: \n" + testJson.toString());
		//System.out.println("JSON2: \n" + testJson2.toString());
	}
	
	@Test
	public void test_03_jackson() throws IOException, JSONException{
		long startTime = System.nanoTime();
		JSONObject testJson2;
		for (int i = 0; i < TEST_RUNS; i++){
			byte[] bytes = JSONByteConverter.toBytesJackson(testJson);
			testJson2 = JSONByteConverter.fromBytesJackson(bytes);
			//assert(testJson.equals(testJson2));
		}
		long endTime = System.nanoTime();
		double avg = (endTime - startTime) / (TEST_RUNS);
		System.out.println("Average byteification time Jackson was " + avg + " nanoseconds.");
		byte[] bytes = JSONByteConverter.toBytesJackson(testJson);
		testJson2 = JSONByteConverter.fromBytesJackson(bytes);

		//System.out.println("JSON1: \n" + testJson.toString());
		//System.out.println("JSON2: \n" + testJson2.toString());
		//assert(testJson.toString().equals(testJson2.toString()));
	}
	
	@Test
	public void test_04_msgpack() throws IOException, JSONException{
		long startTime = System.nanoTime();
		JSONObject testJson2;
		for (int i = 0; i < TEST_RUNS; i++){
			byte[] bytes = JSONByteConverter.toBytesMsgpack(testJson);
			testJson2 = JSONByteConverter.fromBytesMsgpack(bytes);
			//assert(testJson.equals(testJson2));
		}
		long endTime = System.nanoTime();
		double avg = (endTime - startTime) / (TEST_RUNS);
		System.out.println("Average byteification time Jackson was " + avg + " nanoseconds.");
		byte[] bytes = JSONByteConverter.toBytesMsgpack(testJson);
		testJson2 = JSONByteConverter.fromBytesMsgpack(bytes);

		System.out.println("JSON1: \n" + testJson.toString());
		System.out.println("JSON2: \n" + testJson2.toString());
		assert(testJson.toString().equals(testJson2.toString()));
	}
	
	

}
