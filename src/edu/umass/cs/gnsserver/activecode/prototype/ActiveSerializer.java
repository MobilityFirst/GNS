package edu.umass.cs.gnsserver.activecode.prototype;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.json.JSONException;
import org.nustaq.serialization.FSTConfiguration;

import edu.umass.cs.gnsserver.utils.ValuesMap;


/**
 * @author gaozy
 *
 */
public class ActiveSerializer {
	
	FSTConfiguration conf = FSTConfiguration.createDefaultConfiguration();
	ByteArrayOutputStream out = new ByteArrayOutputStream();
	
	protected ActiveSerializer(){
		
	}
	
	protected byte[] serializeFST(ActiveMessage msg) {
		return conf.asByteArray(msg);
	}
	
	protected ActiveMessage deserializeFST(byte[] buffer) {
		return (ActiveMessage) conf.asObject(buffer);
	}
	
	protected byte[] serialize(ActiveMessage obj) {		
	    ObjectOutputStream os;
		try {
			os = new ObjectOutputStream(out);
			os.writeObject(obj);
		} catch (IOException e) {
			e.printStackTrace();
		}
	    
	    return out.toByteArray();
	}
	
	protected ActiveMessage deserialize(byte[] data) {
		ByteArrayInputStream in = new ByteArrayInputStream(data);
	    ObjectInputStream is;
		try {
			is = new ObjectInputStream(in);
			return (ActiveMessage) is.readObject();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	    return null;
	}
	
	/**
	 * @param args
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws JSONException 
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, JSONException{
		ActiveSerializer serializer = new ActiveSerializer();
		
		String guid = "1111111111111111111";
		String field = "22222222222222222";
		String noop_code = "";
		try {
			noop_code = new String(Files.readAllBytes(Paths.get("./scripts/noop.js")));
		} catch (IOException e) {
			e.printStackTrace();
		} 
		
		ValuesMap value = new ValuesMap();
		value.put("string", "hello world");
			
		ActiveMessage msg1 = null;
		
		byte[] buffer1;
		byte[] buffer2;
		
		int n = 1000000;
		long t = System.currentTimeMillis();
		for (int i=0; i<n; i++){
			buffer1 = serializer.serializeFST(new ActiveMessage(guid, field, noop_code, value, 0));
			msg1 = (ActiveMessage) serializer.deserializeFST(buffer1);
			buffer2 = serializer.serializeFST(msg1);
			serializer.deserializeFST(buffer2);
		}
		long elapsed = System.currentTimeMillis() - t;
		System.out.println("It takes "+elapsed+"ms, and the average latency for each operation is "+(elapsed*1000.0/n)+"us");	
	}
}
