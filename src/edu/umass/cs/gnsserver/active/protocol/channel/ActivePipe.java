package edu.umass.cs.gnsserver.active.protocol.channel;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

import org.json.JSONException;
import org.json.simple.JSONObject;


/**
 * @author gaozy
 *
 */
public class ActivePipe implements ActiveChannel{
	
	protected RandomAccessFile pipe = null;
	
	
	/**
	 * @param pfile
	 */
	public ActivePipe(String pfile){
		try {
			pipe = new RandomAccessFile(pfile, "rw");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	private byte[] serialize(Object obj) throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
	    ObjectOutputStream os = new ObjectOutputStream(out);
	    os.writeObject(obj);
	    return out.toByteArray();
	}
	
	private Object deserialize(byte[] data) throws IOException, ClassNotFoundException {
	    ByteArrayInputStream in = new ByteArrayInputStream(data);
	    ObjectInputStream is = new ObjectInputStream(in);
	    return is.readObject();
	}
	
	@Override
	public boolean write(Object obj) throws IOException {
		boolean wSuccess = false;
		byte[] buf = serialize(obj);	    
	    pipe.write(buf);
	    wSuccess = true;
	    return wSuccess;
	}
	
	@Override
	public Object read(byte[] buf) throws IOException, ClassNotFoundException {
	    pipe.read(buf);
	    return deserialize(buf);
	}
	
	static class A implements Serializable {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		
		int a;
		String b;
		String c;
		
		public A(int a, String b, String c){
			this.a = a;
			this.b = b;
			this.c = c;
		}
	}
	
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws IOException, ClassNotFoundException, JSONException{
		
		/**
		 * This test shows the latency without executing active code
		 */
		String noop_code = "";
		try {
			noop_code = new String(Files.readAllBytes(Paths.get("./scripts/activeCode/noop.js")));
		} catch (IOException e) {
			e.printStackTrace();
		} 
		JSONObject value = new JSONObject();		
		value.put("hello", "world");
		
		Object obj1 = new Object();
		int n = 1000000;
		
		final String pfile = "tmp";
		ActivePipe p1 = new ActivePipe(pfile);
		ActivePipe p2 = new ActivePipe(pfile);
		byte[] buf1 = new byte[1024];
		byte[] buf2 = new byte[1024];
		Object obj2 = null;
		Object obj3 = null;
		
		// warm up for one round
		p1.write(obj1);		
		obj2 = p2.read(buf1);
		p2.write(obj2);
		obj3 = p1.read(buf2);
		
		long t = System.currentTimeMillis();
		for (int i=0; i<n; i++){
			p1.write(obj1);
			obj2 = p2.read(buf1);
			Arrays.fill(buf1, (byte) 0);
			p2.write(obj2);
			obj3 = p1.read(buf2);
			Arrays.fill(buf2, (byte) 0);
		}
		long elapsed = System.currentTimeMillis() - t;
		System.out.println("It takes "+elapsed+"ms, and the average latency for each operation is "+(elapsed*1000.0/n)+"us");		
				
		File f = new File(pfile);
		f.delete();
 	}


}
