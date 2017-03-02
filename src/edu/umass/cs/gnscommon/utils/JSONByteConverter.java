/* Copyright (1c) 2016 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (1the "License"); you may not
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
package edu.umass.cs.gnscommon.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map.Entry;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class JSONByteConverter {
	//private static ObjectMapper objectMapper = new ObjectMapper(new MessagePackFactory()).setVisibility(PropertyAccessor.FIELD, Visibility.ANY);
	
	//private static TypeReference<Map<String,Object>> typeRef = new TypeReference<Map<String, Object>>(){};
	//private static TypeReference<JSONObject> typeRef = new TypeReference<JSONObject>(){};
	private static Class<? extends JSONObject> jsonClass = new JSONObject().getClass();
	

	private static final byte STRING_INDICATOR = 0;
	private static final byte ARRAY_INDICATOR = 1;
	private static final byte MAP_INDICATOR = 2;
	private static final byte INTEGER_INDICATOR = 3;
	private static final byte LONG_INDICATOR = 4;
	private static final byte FLOAT_INDICATOR = 5;
	private static final byte DOUBLE_INDICATOR = 6;
	private static final byte BOOLEAN_INDICATOR = 7;
	
	//private static ByteBuffer fourByteBuffer = ByteBuffer.allocate(4);
	//private static ByteBuffer eightByteBuffer = ByteBuffer.allocate(8);

	/**
	 * Checks the type of the value object and either converts it to bytes or recursively handles its elements.
	 * @param value The value to be parsed recursively and packed into the byte array.
	 * @param out The ByteArrayOutputStreeam that is constructing the byte array.
	 * @throws JSONException
	 * @throws IOException
	 */
	private static final void byteJSONValue(Object value, ByteArrayOutputStream out) throws JSONException, IOException{
		ByteBuffer fourByteBuffer = ByteBuffer.allocate(4);
		ByteBuffer eightByteBuffer = ByteBuffer.allocate(8);
		if (value instanceof JSONArray){
			//byteJSONArray((JSONArray)value, out);
			JSONArray array = (JSONArray)value;
			int length = array.length();
			out.write(ARRAY_INDICATOR);
			fourByteBuffer.rewind();
			fourByteBuffer.putInt(length);
			out.write(fourByteBuffer.array());
			for (int i = 0; i < length; i++){
				byteJSONValue(array.get(i),out);
			}
		}
		else if (value instanceof Integer){
			out.write(INTEGER_INDICATOR);
			fourByteBuffer.rewind();
			fourByteBuffer.putInt((Integer)value);
			out.write(fourByteBuffer.array());
		}
		else if (value instanceof Long){
			out.write(LONG_INDICATOR);
			eightByteBuffer.rewind();
			eightByteBuffer.putLong((Long)value);
			out.write(eightByteBuffer.array());
			
		}
		else if (value instanceof Boolean){
			out.write(BOOLEAN_INDICATOR);
			if ((Boolean)value){
				out.write(1);
			}
			else{
				out.write(0);
			}
			
		}
		else if (value instanceof Float){
			out.write(FLOAT_INDICATOR);
			fourByteBuffer.rewind();
			fourByteBuffer.putFloat((Float)value);
			out.write(fourByteBuffer.array());
		}
		else if (value instanceof Double){
			out.write(DOUBLE_INDICATOR);
			eightByteBuffer.rewind();
			eightByteBuffer.putDouble((Double)value);
			out.write(eightByteBuffer.array());
		}
		else if (value instanceof String){
			byte[] stringBytes = ((String)value).getBytes();
			out.write(STRING_INDICATOR);
			int length = stringBytes.length;
			fourByteBuffer.rewind();
			fourByteBuffer.putInt(length);
			out.write(fourByteBuffer.array());
			out.write(stringBytes);
		}
		else if (value instanceof JSONObject){
			//byteJSONObject((JSONObject)value, out);
			JSONObject json = (JSONObject) value;
			@SuppressWarnings("unchecked") // Assumption: All keys in the json are strings.
			Iterator<String> iterator = json.keys();
			int length = json.length();
			out.write(MAP_INDICATOR);
			fourByteBuffer.rewind();
			fourByteBuffer.putInt(length);
			out.write(fourByteBuffer.array());
			while (iterator.hasNext()){
				String key = iterator.next();
				Object val = json.get(key);
				byteJSONValue(key,out);
				byteJSONValue(val,out);
			}
		}
		else{
			throw new JSONException("UNKNOWN TYPE IN JSON!");
		}
	}
	
	private static final void byteJSONValue(Object value, ByteBuffer out) throws JSONException, IOException{
		if (value instanceof JSONArray){
			//byteJSONArray((JSONArray)value, out);
			JSONArray array = (JSONArray)value;
			int length = array.length();
			out.put(ARRAY_INDICATOR);
			out.putInt(length);
			for (int i = 0; i < length; i++){
				byteJSONValue(array.get(i),out);
			}
		}
		else if (value instanceof Integer){
			out.put(INTEGER_INDICATOR);
			out.putInt((Integer)value);
		}
		else if (value instanceof Long){
			out.put(LONG_INDICATOR);
			out.putLong((Long)value);
			
		}
		else if (value instanceof Boolean){
			out.put(BOOLEAN_INDICATOR);
			if ((Boolean)value){
				out.put((byte)1);
			}
			else{
				out.put((byte)0);
			}
			
		}
		else if (value instanceof String){
			byte[] stringBytes = ((String)value).getBytes();
			out.put(STRING_INDICATOR);
			int length = stringBytes.length;
			out.putInt(length);
			out.put(stringBytes);
		}
		else if (value instanceof JSONObject){
			//byteJSONObject((JSONObject)value, out);
			JSONObject json = (JSONObject) value;
			@SuppressWarnings("unchecked") // Assumption: All keys in the json are strings.
			Iterator<String> iterator = json.keys();
			int length = json.length();
			out.put(MAP_INDICATOR);
			out.putInt(length);
			while (iterator.hasNext()){
				String key = iterator.next();
				Object val = json.get(key);
				byteJSONValue(key,out);
				byteJSONValue(val,out);
			}
		}
		else{
			throw new JSONException("UNKNOWN TYPE IN JSON!");
		}
	}

	/**
	 * Converts a JSONObject to bytes recursively.
	 * @param json The JSONObject to be converted to bytes.
	 * @return The byte array representation of the JSONObject.
	 * @throws JSONException
	 * @throws IOException
	 */
	public static final byte[] toBytesHardcoded(JSONObject json) throws JSONException, IOException{
		ByteArrayOutputStream out = new ByteArrayOutputStream(1200);
//		ByteBuffer bbuf = ByteBuffer.wrap(new byte[1200]);
//		byteJSONValue(json, bbuf);
//		byte[] bytes= bbuf.array();
		byteJSONValue(json, out);
		out.close();
		byte[] bytes = out.toByteArray();
		return bytes;
	}
	
	/**
	 * Parses the given bytes and returns the object they represent.
	 * @param bytes to be parsed
	 * @return the object reconstructed from the bytes
	 * @throws JSONException
	 */
	private static Object valueFromBytes(ByteBuffer bytes) throws JSONException{
		byte type = bytes.get();
		//System.out.println("From value: " + new Integer(type).toString());
		switch(type){
		case MAP_INDICATOR:
			//System.out.println("MAP!");
			return mapFromBytes(bytes);
		case ARRAY_INDICATOR:
			return arrayFromBytes(bytes);
		case STRING_INDICATOR:
			//System.out.println("STRING!");
			int length = bytes.getInt();
			byte[] stringBytes = new byte[length];
			bytes.get(stringBytes, 0, length);
			return new String(stringBytes);
		case INTEGER_INDICATOR:
			return bytes.getInt();
		case LONG_INDICATOR:
			return bytes.getLong();
		case DOUBLE_INDICATOR:
			return bytes.getDouble();
		case FLOAT_INDICATOR:
			return bytes.getFloat();
		case BOOLEAN_INDICATOR:
			return bytes.get() == 1 ? 1: 0;
		default:
			throw new JSONException("Tried to decode unknown type from byte array!");
		}
		
			
			
	}
	
	/**
	 * Creates a JSONObject map from the next element of the given bytebuffer
	 * @param bytes the ByteBuffer to be parsed
	 * @return The JSONObject reconstructed from the buffer.
	 * @throws JSONException
	 */
	private static JSONObject mapFromBytes(ByteBuffer bytes) throws JSONException{
		JSONObject json = new JSONObject();
		//System.out.println("From map!");
		int length = bytes.getInt();
		//System.out.println("Length " + Integer.toString(length));
		for (int i = 0; i < length; i++){
			//String key =(String) valueFromBytes(bytes);
			
			//Get Key
			byte type = bytes.get();
			//assert(type == STRING_INDICATOR);
			int lengthKey = bytes.getInt();
			byte[] stringBytes = new byte[lengthKey];
			bytes.get(stringBytes, 0, lengthKey);
			String key = new String(stringBytes);
			//System.out.println("From map: " + new Integer(type).toString());
			//Get value
			type = bytes.get();
			switch(type){
			case MAP_INDICATOR:
				json.put(key, mapFromBytes(bytes));
				break;
			case ARRAY_INDICATOR:
				json.put(key, arrayFromBytes(bytes));
				break;
			case STRING_INDICATOR:
				int lengthString = bytes.getInt();
				stringBytes = new byte[lengthString];
				bytes.get(stringBytes, 0, lengthString);
				json.put(key,new String(stringBytes));
				break;
			case INTEGER_INDICATOR:
				json.put(key,bytes.getInt());
				break;
			case LONG_INDICATOR:
				json.put(key,bytes.getLong());
				break;
			case DOUBLE_INDICATOR:
				json.put(key,bytes.getDouble());
				break;
			case FLOAT_INDICATOR:
				json.put(key,bytes.getFloat());
				break;
			default:
				throw new JSONException("Tried to decode unknown type from byte array!");
			}
		}
		return json;
	}
	
	/**
	 * Creates a JSONArray from the next element of the given bytebuffer
	 * @param bytes the ByteBuffer to be parsed
	 * @return The JSONArray reconstructed from the buffer.
	 * @throws JSONException
	 */
	private static JSONArray arrayFromBytes(ByteBuffer bytes) throws JSONException{
		JSONArray json = new JSONArray();
		int length = bytes.getInt();
		for (int i = 0; i < length; i++){
			//Object value = valueFromBytes(bytes);
			//json.put(value);
			//Get value
			byte type = bytes.get();
			switch(type){
			case MAP_INDICATOR:
				json.put(mapFromBytes(bytes));
				break;
			case ARRAY_INDICATOR:
				json.put(arrayFromBytes(bytes));
				break;
			case STRING_INDICATOR:
				int lengthString = bytes.getInt();
				byte[] stringBytes = new byte[lengthString];
				bytes.get(stringBytes, 0, lengthString);
				json.put(new String(stringBytes));
				break;
			case INTEGER_INDICATOR:
				json.put(bytes.getInt());
				break;
			case LONG_INDICATOR:
				json.put(bytes.getLong());
				break;
			case DOUBLE_INDICATOR:
				json.put(bytes.getDouble());
				break;
			case FLOAT_INDICATOR:
				json.put(bytes.getFloat());
				break;
			default:
				throw new JSONException("Tried to decode unknown type from byte array!");
			}
		}
		return json;
	}
	
	/**
	 * Converts a byte array generated by toBytesHardcoded back into a JSONObject.
	 * @param bytes the byte array to be converted
	 * @return The reconstructed JSONObject
	 * @throws JSONException
	 */
	public static JSONObject fromBytesHardcoded(byte[] bytes) throws JSONException{
		ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
		//byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
		//ByteBuffer byteBuffer = ByteBuffer.allocate(bytes.length);
		//byteBuffer.put(bytes);
		//byteBuffer.rewind();
		JSONObject obj = (JSONObject) valueFromBytes(byteBuffer);
		return obj;
	}

  /**
   *
   * @param bbuf
   * @return a json object
   * @throws JSONException
   */
  public static JSONObject fromBytesHardcoded(ByteBuffer bbuf) throws JSONException{
		return (JSONObject)valueFromBytes(bbuf);
	}

}
