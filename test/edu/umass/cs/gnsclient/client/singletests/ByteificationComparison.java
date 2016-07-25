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
	public byte[] recursiveCollectionToBytes(JSONArray c) throws JSONException, IOException{
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
				byte[] stringBytes = ((String)obj).getBytes();
				int len2 = stringBytes.length;
				out.write(STRING_IDENTIFIER);
				out.write(len2);
				out.write(stringBytes);
			}

		}
		return out.toByteArray();
		
	}
	
	public byte[] recursiveToBytes(JSONObject json) throws JSONException, IOException{
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
				byte[] stringBytes = ((String)obj).getBytes();
				int len2 = stringBytes.length;
				out.write(STRING_IDENTIFIER);
				out.write(len2);
				out.write(stringBytes);
			}

			}
		return out.toByteArray();
	}
	
	/**
	 * 
	 * @param bytes
	 * @param startOffset startOffset Where in the byte array the int that signifies the length of the String in bytes begins.
	 * @return
	 */
	public String stringFromBytes(byte[] bytes, int startOffset){
		int len = (int) (bytes[startOffset] << 24) & (bytes[startOffset+1] << 16) & (bytes[startOffset+2] << 8) & bytes[startOffset+3];
			byte[] stringBytes = new byte[len];
			//First 4 bytes after startOffset are length
			int offset = startOffset+4;
			for (int i = 0; i < len; i++){
				stringBytes[i] = bytes[offset+i];
			}
			return new String(stringBytes);
		
	}
	/**
	 * 
	 * @param bytes
	 * @param startOffset Where in the byte array the int that signifies the length of the collection in elements begins.
	 * @return
	 */
	public Collection<?> collectionFromBytes(byte[] bytes, int startOffset){
		Collection<Object> c = new ArrayList<Object>();
		int len = (int) (bytes[startOffset] << 24) & (bytes[startOffset+1] << 16) & (bytes[startOffset+2] << 8) & bytes[startOffset+3];
		int offset = startOffset+4;//Start after first 4 bytes, which are the length.
		for (int i = 0; i < len; i++){
			short identifier = (short) ((bytes[offset] << 8) & bytes[offset+1]);
			if (identifier == COLLECTION_IDENTIFIER){
				Collection<?> newCollection = collectionFromBytes(bytes, offset+2);
				c.add(newCollection);
			}
			else if (identifier == STRING_IDENTIFIER){
				String newString = stringFromBytes(bytes, offset+2);
				c.add(newString);
			}
			else{
				fail("Invalid identifier during reconstruction of " + bytes.toString() + " at index " + Integer.toString(offset));
			}
		}
		return c;
	}
	
	public JSONObject recreateRecursive(byte[] bytes) throws JSONException{
		JSONObject json = new JSONObject();
		//Assuming Big Endian byte representation, pattern is 2 bytes identifier, 4 bytes length integer.
		short identifier = (short) ((bytes[0] << 8) & bytes[1]);
		int len = (int) (bytes[2] << 24) & (bytes[3] << 16) & (bytes[4] << 8) & bytes[5];
		
		//First 2 bytes are the type
		String key = stringFromBytes(bytes, 2);
		//First 6 bytes are array type and length for the key, next "len" are the key, next 2 are next identifier for the value.
		int offset = 6+len;
		identifier = (short) ((bytes[offset] << 8) & bytes[offset+1]);

		offset = offset+2; //Move offset to after the identifier.			
		switch(identifier){
		case STRING_IDENTIFIER:
			String value = stringFromBytes(bytes, offset);
			json.put(key, value);
			break;
		case COLLECTION_IDENTIFIER:
			Collection<?> collection = collectionFromBytes(bytes, offset);
			json.put(key, collection);
			break;
		}

		
		return json;
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
		System.out.println("JSON1: \n" + testJson.toString());
		System.out.println("JSON2: \n" + testJson2.toString());
		
	}
	
	@Test
	public void test_02_recursive() throws JSONException, IOException {
		long startTime = System.nanoTime();
		for (int i = 0; i < TEST_RUNS; i++){
			byte[] bytes = recursiveToBytes(testJson);
			JSONObject testJson2 = recreateRecursive(bytes);
			//assert(testJson.equals(testJson2));
		}
		long endTime = System.nanoTime();
		double avg = (endTime - startTime) / (TEST_RUNS);
		System.out.println("Average byteification time RECURSIVE was " + avg + " nanoseconds.");
		
		byte[] bytes = testJson.toString().getBytes();
		JSONObject testJson2 = new JSONObject(new String(bytes));
		System.out.println("JSON1: \n" + testJson.toString());
		System.out.println("JSON2: \n" + testJson2.toString());
	}

}
