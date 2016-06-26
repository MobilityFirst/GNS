package edu.umass.cs.gnsserver.activecode.prototype;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.ProcessBuilder.Redirect;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.json.JSONException;

import edu.umass.cs.gnsserver.utils.ValuesMap;

/**
 * @author gaozy
 *
 */
public class ActiveHandler {
	
	private static ActiveChannel channel;
	private String ifile;
	private String ofile;
	byte[] buffer = new byte[ActiveWorker.bufferSize];
	
	private Process workerProc;
	final static int total = 1;
	protected static int received = 0;
	synchronized int incr(){
		return ++received;
	}
	
	
	protected ActiveHandler(String ifile, String ofile){
		this.ifile = ifile;
		this.ofile = ofile;
		
		Runtime runtime = Runtime.getRuntime();
		try {
			runtime.exec("mkfifo "+ifile);
			runtime.exec("mkfifo "+ofile);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
				
		try {
			workerProc = startWorker(ofile, ifile);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		channel = new ActivePipe(ifile, ofile);
		
		System.out.println("Start handler by listening on "+ifile+", and write to "+ofile+".Handler's channel is ready!");
	}
	
	
	protected void shutdown(){
		
		if(workerProc != null){
			workerProc.destroy();
		}
		
		(new File(ifile)).delete();
		(new File(ofile)).delete();
	}
	
	private static Process startWorker(String ifile, String ofile) throws IOException{
		List<String> command = new ArrayList<String>();
		String classpath = System.getProperty("java.class.path");
	    command.add("java");
	    command.add("-Xms64m");
	    command.add("-Xmx64m");
	    command.add("-cp");
	    command.add(classpath);
	    command.add("edu.umass.cs.gnsserver.activecode.prototype.ActiveWorker");
	    command.add(ifile);
	    command.add(ofile);
		
	    ProcessBuilder builder = new ProcessBuilder(command);
		builder.directory(new File(System.getProperty("user.dir")));
		
		builder.redirectError(Redirect.INHERIT);
		builder.redirectOutput(Redirect.INHERIT);
		builder.redirectInput(Redirect.INHERIT);
		
		Process process = builder.start();		
		System.out.println("Worker Start ...");
		return process;
	}
	
	protected ActiveMessage read(){
		channel.read(buffer);
		ActiveMessage am = null;
		try {
			am = new ActiveMessage(buffer);
		} catch (UnsupportedEncodingException | JSONException e) {
			e.printStackTrace();
		}
		Arrays.fill(buffer, (byte) 0); 
		return am;
	}
	
	protected boolean write(ActiveMessage am){
		boolean wSuccess = false;
		try {
			byte[] buf = am.toBytes();
			channel.write(buf, 0, buf.length);
			wSuccess = true;
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}		
		return wSuccess;
	}
	
	/**
	 * @param args
	 * @throws InterruptedException 
	 * @throws JSONException 
	 */
	public static void main(String[] args) throws InterruptedException, JSONException{
		String cfile = "/tmp/client";
		String sfile = "/tmp/server";		
		
		ActiveHandler handler = new ActiveHandler(cfile, sfile);

		Thread.sleep(1000);
		
		String guid = "guid";
		String field = "name";
		String noop_code = "";
		try {
			noop_code = new String(Files.readAllBytes(Paths.get("./scripts/activeCode/noop.js")));
		} catch (IOException e) {
			e.printStackTrace();
		} 
		ValuesMap value = new ValuesMap();
		value.put("string", "hello world!");	
		
		int n = 1000000;
		ActiveMessage msg = new ActiveMessage(guid, field, noop_code, value, 0);
		
		long t1 = System.currentTimeMillis();
		for (int i=0; i<n; i++){				
			handler.write(msg);
			ActiveMessage am = handler.read();
			//System.out.println("received:"+am.toString());
		}
		
		long elapsed = System.currentTimeMillis() - t1;
		System.out.println("It takes "+elapsed+"ms, and the average latency for each operation is "+(elapsed*1000.0/n)+"us");
		
		handler.shutdown();
	}

	
}
