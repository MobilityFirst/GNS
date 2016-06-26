package edu.umass.cs.gnsserver.activecode.prototype;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gnsserver.utils.ValuesMap;

/**
 * @author gaozy
 *
 */
public class ActiveMessage {
	
	private final static String CHARSET = "ISO-8859-1";
	
	private boolean finished;
	private int ttl;
	private final String guid;
	private final String field;
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
	 * @param targetGuid 
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
	
	protected void setValue(ValuesMap value){
		this.value = value;
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
	
	protected byte[] toBytes() throws UnsupportedEncodingException{
		// First convert ValuesMap to String, as it is costly
		String valuesMapString = (value == null)?null:value.toString();
		
		byte[] buffer = new byte[this.getEstimatedLengthExceptForValuesMap()+( (valuesMapString==null)?0:valuesMapString.length() )];
		ByteBuffer bbuf = ByteBuffer.wrap(buffer);
		
		int exactLength = 0;
		// put ttl and finished
		bbuf.putInt(ttl);
		bbuf.put(this.finished? (byte) 1: (byte) 0 );
		exactLength += (Integer.BYTES + 1);
		
		// put guid, can't be null, ~100ns
		assert(guid != null);
		byte[] guidBytes = guid.getBytes(CHARSET);
		bbuf.putInt(guidBytes.length);
		bbuf.put(guidBytes);
		exactLength += (Integer.BYTES + guidBytes.length);
		
		// put field, can't be null, ~100ns
		assert(field != null);
		byte[] fieldBytes = field.getBytes(CHARSET);
		bbuf.putInt(fieldBytes.length);
		bbuf.put(fieldBytes);
		exactLength += (Integer.BYTES + fieldBytes.length);
		
		// put targetGuid, can be null, ~100ns
		byte[] targetGuidBytes = (targetGuid==null)? new byte[0]:targetGuid.getBytes(CHARSET);
		bbuf.putInt((targetGuid==null)?0:targetGuidBytes.length);
		bbuf.put(targetGuidBytes);
		exactLength += (Integer.BYTES + ((targetGuid==null)? 0:targetGuidBytes.length));
		
		// put code, can be null
		byte[] codeBytes = (code==null)? new byte[0]:code.getBytes(CHARSET);
		bbuf.putInt( (code==null)?0:codeBytes.length );
		bbuf.put(codeBytes);
		exactLength += (Integer.BYTES + ((code==null)? 0:codeBytes.length));
		
		// put valuesMapString, can be null
		byte[] valuesMapBytes = (valuesMapString==null)? new byte[0]:valuesMapString.getBytes();
		bbuf.putInt( (valuesMapString==null)?0:valuesMapBytes.length );
		bbuf.put(valuesMapBytes);
		exactLength += (Integer.BYTES + ((valuesMapString==null)? 0:valuesMapBytes.length));
		
		byte[] exactBytes = new byte[exactLength];
		bbuf.flip();
		assert (bbuf.remaining() == exactLength) : bbuf.remaining() + " != " + exactLength;
		
		bbuf.get(exactBytes);
		return exactBytes;
	}
	
	/**
	 * @param bytes
	 * @throws UnsupportedEncodingException 
	 * @throws JSONException 
	 */
	public ActiveMessage(byte[] bytes) throws UnsupportedEncodingException, JSONException {
		this(ByteBuffer.wrap(bytes));
	}
	
	/**
	 * @param bbuf
	 * @throws UnsupportedEncodingException 
	 * @throws JSONException 
	 */
	public ActiveMessage(ByteBuffer bbuf) throws UnsupportedEncodingException, JSONException {
		this.ttl = bbuf.getInt();
		this.finished = bbuf.get() == (byte) 1;
		
		// get guid
		int length = bbuf.getInt();
		byte[] guidBytes = new byte[length];
		bbuf.get(guidBytes);
		this.guid = new String(guidBytes, CHARSET);
		
		// get field
		length = bbuf.getInt();
		byte[] fieldBytes = new byte[length];
		bbuf.get(fieldBytes);
		this.field = new String(fieldBytes, CHARSET);
		
		// get targetGuid
		length = bbuf.getInt();
		if(length>0){
			byte[] targetGuidBytes = new byte[length];
			bbuf.get(targetGuidBytes);
			this.targetGuid = new String(targetGuidBytes, CHARSET);
		}
		
		// get code
		length = bbuf.getInt();
		if(length>0){
			byte[] codeBytes = new byte[length];
			bbuf.get(codeBytes);
			this.code = new String(codeBytes, CHARSET);
		}
		
		// get valuesMap
		length = bbuf.getInt();
		if(length>0) {
			byte[] valueBytes = new byte[length];
			bbuf.get(valueBytes);
			this.value = new ValuesMap(new JSONObject(new String(valueBytes, CHARSET)));
		}
	}
	
	@Override
	public String toString(){
		
		return "[guid:"+this.guid
				+",field:"+this.field
				+",value:"+this.value.toString()
				+"]";
	}
	
	/**
	 * @param args
	 * @throws JSONException
	 * @throws UnsupportedEncodingException 
	 */
	public static void main(String[] args) throws JSONException, UnsupportedEncodingException{
		
		String guid = "zhaoyu";
		String field = "gao";
		String noop_code = "";
		try {
			noop_code = new String(Files.readAllBytes(Paths.get("./scripts/activeCode/noop.js")));
		} catch (IOException e) {
			e.printStackTrace();
		} 
		ValuesMap value = new ValuesMap();
		value.put("string", "hello world");
		
		int n = 1000000;
		long t = System.currentTimeMillis();
		/*
		for (int i=0; i<n; i++){
			new ActiveMessage(guid, field, noop_code, value, 0);
		}
		long elapsed = System.currentTimeMillis() - t;		
		System.out.println("It takes "+elapsed+"ms for create 1m ActiveMessage, and the average latency for each operation is "+(elapsed*1000.0/n)+"us");		
		*/
		
		ActiveMessage amsg = new ActiveMessage(guid, field, noop_code, value, 0);
		ActiveMessage rmsg = null;
		
		t = System.currentTimeMillis();
		for (int i=0; i<n; i++){
			rmsg = new ActiveMessage(amsg.toBytes());
			new ActiveMessage(rmsg.toBytes());
		}
		long elapsed = System.currentTimeMillis() - t;
		System.out.println("It takes "+elapsed+"ms for serialize and deserialize 1m ActiveMessage, and the average latency for each operation is "+(elapsed*1000.0/n)+"us");	
		//System.out.println(rmsg.getGuid()+" "+rmsg.getField()+" "+rmsg.getCode()+" "+rmsg.getValue());
	}

}
