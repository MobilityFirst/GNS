package edu.umass.cs.gnsserver.activecode.prototype;

import java.io.File;
import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;


import org.json.JSONException;

import edu.umass.cs.gnsserver.activecode.prototype.ActiveMessage.Type;
import edu.umass.cs.gnsserver.activecode.prototype.interfaces.Channel;
import edu.umass.cs.gnsserver.activecode.prototype.interfaces.Client;
import edu.umass.cs.gnsserver.interfaces.ActiveDBInterface;
import edu.umass.cs.gnsserver.utils.ValuesMap;

/**
 * @author gaozy
 *
 */
public class ActiveClient implements Client{
	
	private ActiveQueryHandler queryHandler;
	
	private Channel channel;
	private String ifile;
	private String ofile;
	
	
	private Process workerProc;
	final private int id;
	final private boolean pipeEnable;
	
	private static int heapSize = 64;
	
	/************** Test Only ******************/
	final ActiveWorker worker;	
	
	/**
	 * @param app 
	 * @param ifile
	 * @param ofile
	 * @param id 
	 * @param workerNumThread 
	 */
	public ActiveClient(ActiveDBInterface app, String ifile, String ofile, int id, int workerNumThread){
		this.id = id;
		this.ifile = ifile;
		this.ofile = ofile;
		this.pipeEnable = true;
		
		Runtime runtime = Runtime.getRuntime();
		try {
			runtime.exec("mkfifo "+ifile);
			runtime.exec("mkfifo "+ofile);
		} catch (IOException e1) {
			e1.printStackTrace();
		}		
		try {
			workerProc = startWorker(ofile, ifile, id, workerNumThread);
		} catch (IOException e) {
			e.printStackTrace();
		}
		channel = new ActiveNamedPipe(ifile, ofile);				
		System.out.println("Start "+this+" by listening on "+ifile+", and write to "+ofile);
		
		queryHandler = new ActiveQueryHandler(app);
		
		worker = new ActiveWorker(null, null, 0, 1, true);
	}
	
	/**
	 * Initialize a client with a UDP channel
	 * @param app
	 * @param port
	 * @param serverPort
	 * @param id
	 * @param workerNumThread
	 */
	public ActiveClient(ActiveDBInterface app, int port, int serverPort, int id, int workerNumThread){
		this.pipeEnable = false;
		this.id = id;
		
		try {
			// reverse the order of port and serverPort, so that worker 
			workerProc = startWorker(serverPort, port, id, workerNumThread);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		channel = new ActiveDatagramChannel(port, serverPort);
		queryHandler = new ActiveQueryHandler(app);
		
		worker = new ActiveWorker(null, null, 0, 1, true);
	}
	
	/**
	 * @param app 
	 * @param ifile
	 * @param ofile
	 */
	public ActiveClient(ActiveDBInterface app, String ifile, String ofile){
		this(app, ifile, ofile, 0, 1);
	}
	
	/**
	 * Destroy the worker process if it's still running,
	 * delete the 
	 */
	@Override
	public void shutdown(){
		if(workerProc != null){
			workerProc.destroy();
		}
		
		if(pipeEnable){
			(new File(ifile)).delete();
			(new File(ofile)).delete();
		}
		
		channel.shutdown();
	}
	
	/**
	 * Create a worker with named pipe
	 * @param ifile
	 * @param ofile
	 * @param id
	 * @param workerNumThread
	 * @return
	 * @throws IOException
	 */
	private Process startWorker(String ifile, String ofile, int id, int workerNumThread) throws IOException{
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
	    command.add(""+workerNumThread);
	    command.add(Boolean.toString(pipeEnable));
	    
	    ProcessBuilder builder = new ProcessBuilder(command);
		builder.directory(new File(System.getProperty("user.dir")));
		
		builder.redirectError(Redirect.INHERIT);
		builder.redirectOutput(Redirect.INHERIT);
		//builder.redirectInput(Redirect.INHERIT);
		
		Process process = builder.start();
		return process;
	}
	
	/**
	 * Create a worker with UDP channel
	 * 
	 * @param port1
	 * @param id
	 * @param workerNumThread
	 * @return
	 * @throws IOException
	 */
	private Process startWorker(int port1, int port2, int id, int workerNumThread) throws IOException{
		List<String> command = new ArrayList<String>();
		String classpath = System.getProperty("java.class.path");
	    command.add("java");
	    command.add("-Xms"+heapSize+"m");
	    command.add("-Xmx"+heapSize+"m");
	    command.add("-cp");
	    command.add(classpath);
	    command.add("edu.umass.cs.gnsserver.activecode.prototype.ActiveWorker");
	    command.add(""+port1);
	    command.add(""+port2);
	    command.add(""+id);
	    command.add(""+workerNumThread);
		command.add(Boolean.toString(pipeEnable));
		
	    ProcessBuilder builder = new ProcessBuilder(command);
		builder.directory(new File(System.getProperty("user.dir")));
		
		builder.redirectError(Redirect.INHERIT);
		builder.redirectOutput(Redirect.INHERIT);
		//builder.redirectInput(Redirect.INHERIT);
		
		Process process = builder.start();
		return process;
	}
	
	protected ActiveMessage receiveMessage(){		
		ActiveMessage am = null;
		try {
			am = (ActiveMessage) channel.receiveMessage();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return am;
	}
	
	protected void sendMessage(ActiveMessage am){
		try {
			channel.sendMessage(am);
		} catch (IOException e) {
			e.printStackTrace();
		}
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
	@Override
	public synchronized ValuesMap runCode( String guid, String field, String code, ValuesMap valuesMap, int ttl) throws ActiveException {
		/*
		if(field.equals("level3")){
			//FIXME: this test can be removed together with worker variable
			try {
				return worker.runCode(guid, field, code, valuesMap, ttl);
			} catch (NoSuchMethodException | ScriptException e) {
				e.printStackTrace();
			}
		}
		*/
		ActiveMessage msg = new ActiveMessage(guid, field, code, valuesMap, ttl);
		sendMessage(msg);
		ActiveMessage response;
		while((response = receiveMessage()) != null){			
			if(response.type==Type.RESPONSE){
				// this is the response
				break;
			}else{
				//FIXME: for test only
				if(response.getField().equals("nextGuid")){
					ValuesMap map = new ValuesMap();
					try {
						map.put("nextGuid", "");
						sendMessage(new ActiveMessage(response.getId(), map, null));
						continue;
					} catch (JSONException e) {
						e.printStackTrace();
					}
				}
				ActiveMessage am = null;
				if(response.type==Type.READ_QUERY){
					am = queryHandler.handleReadQuery(response);
				} else {
					am = queryHandler.handleWriteQuery(response);
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
	 * @throws ActiveException 
	 */
	public static void main(String[] args) throws InterruptedException, JSONException, ActiveException{
		
		String suffix = "";
		/*
		if (args.length == 1)
			suffix = args[0];
		if (args.length == 2) {
			suffix = args[0];
			heapSize = Integer.parseInt(args[1]);
		}*/
		int numThread = 1; //Integer.parseInt(args[0]);
		
		
		String cfile = "/tmp/client"+suffix;
		String sfile = "/tmp/server"+suffix;		
		/**
		 * Test client performance with named pipe channel
		 */
		ActiveClient client = new ActiveClient(null, cfile, sfile, 0, numThread);
		
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
		System.out.println("The average throughput is "+(n*1000.0/elapsed)*numThread);
		client.shutdown();
	}

	
}
