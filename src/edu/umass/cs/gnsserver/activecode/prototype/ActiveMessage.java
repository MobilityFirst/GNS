package edu.umass.cs.gnsserver.activecode.prototype;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.json.JSONException;
import org.json.simple.JSONObject;

import edu.umass.cs.gnsserver.utils.ValuesMap;

/**
 * @author gaozy
 *
 */
public class ActiveMessage {
	
	private final static String CHARSET = "ISO-8859-1";
	
	private boolean finished;
	private int ttl;
	private String guid;
	private String field;
	private String code;
	private ValuesMap value;
	private String targetGuid;

	/**
	 * @param guid
	 * @param field
	 * @param code
	 * @param ttl
	 * @param value 
	 * @param finished 
	 */
	public ActiveMessage(String guid, String field, String code, ValuesMap value, int ttl, boolean finished, String targetGuid){
		this.guid = guid;
		this.field = field;
		this.code = code;
		this.ttl = ttl;
		this.value = value;
		this.finished = finished;
		this.targetGuid = targetGuid;
	}
	
	/**
	 * @param guid
	 * @param field
	 * @param code
	 * @param value
	 * @param ttl
	 * @param finished
	 */
	public ActiveMessage(String guid, String field, String code, ValuesMap value, int ttl, boolean finished){
		this(guid, field, code, value, ttl, finished, null);
	}
	
	/**
	 * @param guid
	 * @param field
	 * @param code
	 * @param value
	 * @param ttl
	 */
	public ActiveMessage(String guid, String field, String code, ValuesMap value, int ttl){
		this(guid, field, code, value, ttl, false, null);
	}
	
	protected int getTtl() {
		return ttl;
	}

	protected String getGuid() {
		return guid;
	}

	protected String getField() {
		return field;
	}

	protected String getCode() {
		return code;
	}

	protected ValuesMap getValue() {
		return value;
	}

	protected boolean isFinished() {
		return finished;
	}
	
	private int getEstimatedLengthExceptForValuesMap(){
		int length = 5*Integer.BYTES // ttl, guid length, field length, code length, valuesMap size
				+ 1 // finished
				+ guid.length() // guid
				+ field.length() // field
				+ code.length(); //code

		return (int) (length*1.25);
	}
	
	protected byte[] toBytes(){
		// First convert ValuesMap to String, as it is costly
		String valuesMapString = (value == null)?"":value.toString();
		
		byte[] buffer = new byte[this.getEstimatedLengthExceptForValuesMap()+valuesMapString.length()];
		ByteBuffer bbuf = ByteBuffer.wrap(buffer);
		
		int exactLength = 0;
		// put ttl and finished
		bbuf.putInt(ttl);
		bbuf.put(this.finished? (byte) 1: (byte) 0 );
		exactLength += (Integer.BYTES + 1);
		
		// put guid, can't be null
		
		// put field, can't be null
		
		// put targetGuid, can be null
		
		// put code, can be null
		
		// put valuesMap, can be null
		
		byte[] exactBytes = new byte[exactLength];
		bbuf.get(exactBytes);
		return exactBytes;
	}
	
	public ActiveMessage(byte[] bytes) {
		this(ByteBuffer.wrap(bytes));
	}
	
	public ActiveMessage(ByteBuffer bbuf) {
		
	}
	
	public static void main(String[] args) throws JSONException{
		
		String guid = "1";
		String field = "2";
		String noop_code = "";
		try {
			noop_code = new String(Files.readAllBytes(Paths.get("./scripts/noop.js")));
		} catch (IOException e) {
			e.printStackTrace();
		} 
		ValuesMap value = new ValuesMap();
		value.put("string", "hello world");
		
		int n = 1000000;
		long t = System.currentTimeMillis();
		for (int i=0; i<n; i++){
			new ActiveMessage(guid, field, noop_code, value, 0);
		}
		long elapsed = System.currentTimeMillis() - t;
		System.out.println("It takes "+elapsed+"ms, and the average latency for each operation is "+(elapsed*1000.0/n)+"us");		
				
	}

}
