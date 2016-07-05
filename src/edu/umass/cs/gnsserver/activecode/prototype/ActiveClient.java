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

import edu.umass.cs.gnsserver.activecode.prototype.ActiveMessage.Type;
import edu.umass.cs.gnsserver.activecode.prototype.interfaces.ActiveChannel;
import edu.umass.cs.gnsserver.interfaces.ActiveDBInterface;
import edu.umass.cs.gnsserver.utils.ValuesMap;

/**
 * @author gaozy
 *
 */
public class ActiveClient {
	
	private ActiveQueryHandler queryHandler;
	
	private ActiveChannel channel;
	private String ifile;
	private String ofile;
	private final byte[] buffer = new byte[ActiveWorker.bufferSize];
	
	private Process workerProc;
	final private int id;
	
	private static int heapSize = 64;
	
	
	
	/************** Test Only ******************/
	//ActiveWorker worker;
	
	
	/**
	 * @param app 
	 * @param ifile
	 * @param ofile
	 * @param id 
	 */
	public ActiveClient(ActiveDBInterface app, String ifile, String ofile, int id){
		this.id = id;
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
			workerProc = startWorker(ofile, ifile, id);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		channel = new ActivePipe(ifile, ofile);
		
		System.out.println("Start "+this+" by listening on "+ifile+", and write to "+ofile);
		
		queryHandler = new ActiveQueryHandler(app);
	}
	
	/**
	 * @param app 
	 * @param ifile
	 * @param ofile
	 */
	public ActiveClient(ActiveDBInterface app, String ifile, String ofile){
		this(app, ifile, ofile, 0);
	}
	
	protected void shutdown(){
		if(workerProc != null){
			workerProc.destroy();
		}
		
		(new File(ifile)).delete();
		(new File(ofile)).delete();
	}
	
	private Process startWorker(String ifile, String ofile, int id) throws IOException{
		List<String> command = new ArrayList<String>();
		String classpath = System.getProperty("java.class.path");
	    command.add("java");
	    command.add("-Xms"+heapSize+"m");
	    command.add("-Xmx"+heapSize+"m");
	    command.add("-cp");
	    command.add(classpath);
	    command.add("edu.umass.cs.gnsserver.activecode.prototype.ActiveWorker");
	    command.add(ifile);
	    command.add(ofile);
	    command.add(""+id);
		
	    ProcessBuilder builder = new ProcessBuilder(command);
		builder.directory(new File(System.getProperty("user.dir")));
		
		builder.redirectError(Redirect.INHERIT);
		builder.redirectOutput(Redirect.INHERIT);
		//builder.redirectInput(Redirect.INHERIT);
		
		Process process = builder.start();		
		//System.out.println("Worker Start ...");
		return process;
	}
	
	protected ActiveMessage receiveMessage(){
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
	
	protected boolean sendMessage(ActiveMessage am){
		boolean wSuccess = false;
		try {
			byte[] buf = am.toBytes();		
			wSuccess = channel.write(buf, 0, buf.length);
			//System.out.println(this+" seccessfully sent "+am+" to "+ofile);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}		
		return wSuccess;
	}
	
	static int numReq = 0;
	synchronized int incr(){
		return ++numReq;
	}
	
	static int received = 0;
	synchronized int getRcv(){
		return ++received;
	}
	
	/**
	 * @param guid
	 * @param field
	 * @param code
	 * @param valuesMap
	 * @param ttl
	 * @return executed result sent back from worker
	 */
	public synchronized ValuesMap runCode( String guid, String field, String code, ValuesMap valuesMap, int ttl){
		/*
		if(field.equals("level3")){
			try {
				return worker.runCode(guid, field, code, valuesMap);
			} catch (NoSuchMethodException | ScriptException e) {
				e.printStackTrace();
			}
		}
		*/
		ActiveMessage msg = new ActiveMessage(guid, field, code, valuesMap, ttl);
		sendMessage(msg);
		ActiveMessage response;
		while(true){
			response = receiveMessage();
			if(response.type==Type.RESPONSE){
				// this is the response
				break;
			}else{
				//FIXME: for test only
				if(response.getField().equals("nextGuid")){
					ValuesMap map = new ValuesMap();
					try {
						map.put("nextGuid", "");
						sendMessage(new ActiveMessage(map, null));
						continue;
					} catch (JSONException e) {
						e.printStackTrace();
					}
				}
				ActiveMessage am = null;
				if(response.type==Type.READ_QUERY){
					am = queryHandler.handleReadQuery(response.getGuid(), response.getTargetGuid(), response.getField(), response.getTtl());

				} else {
					am = queryHandler.handleWriteQuery(response.getGuid(), response.getTargetGuid(), response.getField(), response.getValue(), response.getTtl());
				}
				sendMessage(am);
			}
		}
		return response.getValue();
	}
	
	public String toString(){
		return this.getClass().getSimpleName()+id;
	}
	
	/**
	 * @param args
	 * @throws InterruptedException 
	 * @throws JSONException 
	 */
	public static void main(String[] args) throws InterruptedException, JSONException{
		
		String suffix = "";
		if (args.length == 1)
			suffix = args[0];
		if (args.length == 2)
		{
			suffix = args[0];
			heapSize = Integer.parseInt(args[1]);
		}
		
		String cfile = "/tmp/client"+suffix;
		String sfile = "/tmp/server"+suffix;		
		
		ActiveClient client = new ActiveClient(null, cfile, sfile);
		//initialize a new client for test
	    //new ActiveClient(cfile+"1", sfile+"1");
		
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
		//ActiveMessage msg = new ActiveMessage(guid, field, noop_code, value, 0);
		
		long t1 = System.currentTimeMillis();
		
		for (int i=0; i<n; i++){
			client.runCode(guid, field, noop_code, value, 0);
		}
		
		long elapsed = System.currentTimeMillis() - t1;
		System.out.println("It takes "+elapsed+"ms, and the average latency for each operation is "+(elapsed*1000.0/n)+"us");
		
		client.shutdown();
	}

	
}
