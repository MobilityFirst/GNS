package edu.umass.cs.gnsserver.activecode.prototype.blocking;


import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gnsserver.activecode.ActiveCodeConfig;
import edu.umass.cs.gnsserver.activecode.ActiveCodeHandler;
import edu.umass.cs.gnsserver.activecode.prototype.ActiveException;
import edu.umass.cs.gnsserver.activecode.prototype.ActiveMessage;
import edu.umass.cs.gnsserver.activecode.prototype.ActiveMessage.Type;
import edu.umass.cs.gnsserver.activecode.prototype.ActiveQueryHandler;
import edu.umass.cs.gnsserver.activecode.prototype.channels.ActiveDatagramChannel;
import edu.umass.cs.gnsserver.activecode.prototype.channels.ActiveNamedPipe;
import edu.umass.cs.gnsserver.activecode.prototype.interfaces.Channel;
import edu.umass.cs.gnsserver.activecode.prototype.interfaces.Client;
import edu.umass.cs.gnsserver.interfaces.ActiveDBInterface;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;
import edu.umass.cs.gnsserver.utils.ValuesMap;
import edu.umass.cs.utils.DelayProfiler;

/**
 * This is a Client implementation with unix named pipe as the way
 * to communicate with workers.
 * 
 * This client send requests to its worker and block the sending thread. 
 * Until it receives the response, the runCode is done. 
 * <p> If it receives a query, it call QueryHandler to handle the query.
 * <p> If it receives a null value, it knows the worker is crashed and
 * it needs to restart a new worker. This design relies on the fact that
 * if the writer end of a named pipe is closed, the
 * reader end will also be closed, and return a {@code null} value.
 *
 * @author gaozy
 *
 */
public class ActiveBlockingClient implements Client {
	
	private final static int DEFAULT_HEAP_SIZE = ActiveCodeConfig.activeWorkerHeapSize;
	private final static String ACTION_ON_OUT_OF_MEMORY = "kill -9 %p";
	
	private ActiveQueryHandler queryHandler;
	
	private final String nodeId;
	private Channel channel;
	private final String ifile;
	private final String ofile;
	private final int workerNumThread;
	
	private Process workerProc;
	private final int id;
	private final boolean pipeEnable;
	private final static boolean CRASH_ENABLED = ActiveCodeConfig.activeCrashEnabled;
	
	private final int heapSize;
	
	private AtomicBoolean isRestarting = new AtomicBoolean();
	
	/********************* For test **********************/
	/**
	 * @return current worker process
	 */
	public Process getWorker(){
		return workerProc;
	}
	
	private AtomicInteger counter = new AtomicInteger(0);
	
	/**
	 * 
	 * @return the total number of received responses
	 */
	public int getRecv(){
		return counter.get();
	}
	
	/**
	 * @param nodeId 
	 * @param app 
	 * @param ifile
	 * @param ofile
	 * @param id 
	 * @param workerNumThread 
	 * @param heapSize 
	 */
	public ActiveBlockingClient(String nodeId, ActiveDBInterface app, String ifile, String ofile, int id, int workerNumThread, int heapSize){
		this.nodeId = nodeId;
		this.id = id;
		this.ifile = ifile;
		this.ofile = ofile;
		this.pipeEnable = true;
		this.workerNumThread = workerNumThread;
		this.heapSize = heapSize;
		
		initializeChannelAndStartWorker();
		
		queryHandler = new ActiveQueryHandler(app);
		
	}
	
	/**
	 * @param app
	 * @param ifile
	 * @param ofile
	 * @param id
	 * @param workerNumThread
	 * @param nodeId 
	 */
	public ActiveBlockingClient(String nodeId, ActiveDBInterface app, String ifile, String ofile, int id, int workerNumThread){
		this(nodeId, app, ifile, ofile, id, workerNumThread, DEFAULT_HEAP_SIZE);
	}
	
	private void initializeChannelAndStartWorker(){
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
		channel = new ActiveNamedPipe(ifile, ofile);				
	}
	
	/**
	 * Initialize a client with a UDP channel
	 * @param nodeId 
	 * @param app
	 * @param port
	 * @param serverPort
	 * @param id
	 * @param workerNumThread
	 */
	public ActiveBlockingClient(String nodeId, ActiveDBInterface app, int port, int serverPort, int id, int workerNumThread){
		this.nodeId = nodeId;
		this.pipeEnable = false;
		this.id = id;
		this.workerNumThread = workerNumThread;
		this.ifile = null;
		this.ofile = null;
		this.heapSize = DEFAULT_HEAP_SIZE;
		
		try {
			// reverse the order of port and serverPort, so that worker 
			workerProc = startWorker(serverPort, port, id);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		channel = new ActiveDatagramChannel(port, serverPort);
		queryHandler = new ActiveQueryHandler(app);
		
	}
	
	/**
   * @param nodeId
	 * @param app 
	 * @param ifile
	 * @param ofile
	 */
	public ActiveBlockingClient(String nodeId, ActiveDBInterface app, String ifile, String ofile){
		this(nodeId, app, ifile, ofile, 0, 1);
	}
	
	/**
	 * Destroy the worker process if it's still running,
	 * delete the 
	 */
	@Override
	public void shutdown(){
		
		if(workerProc != null){		
			//FIXME: forcibly kill the worker
			workerProc.destroyForcibly();						
		}
		
		if(pipeEnable){
			(new File(ifile)).delete();
			(new File(ofile)).delete();
		}
		
		channel.close();
	}
	
	/**
	 * Create a worker with named pipe
	 * @param ifile
	 * @param ofile
	 * @param id
	 * @param workerNumThread
	 * @return a Process
	 * @throws IOException
	 */
	private Process startWorker(String ifile, String ofile, int id) throws IOException{
		List<String> command = new ArrayList<>();
		String classpath = System.getProperty("java.class.path");
	    command.add("java");
	    command.add("-Xms"+heapSize+"m");
	    command.add("-Xmx"+heapSize+"m");
	    // kill the worker on OutOfMemoryError
	    if(CRASH_ENABLED) {
              command.add("-XX:OnOutOfMemoryError="+ACTION_ON_OUT_OF_MEMORY);
            }
	    command.add("-cp");
	    command.add(classpath);
	    command.add("edu.umass.cs.gnsserver.activecode.prototype.blocking.ActiveBlockingWorker");
	    command.add(ifile);
	    command.add(ofile);
	    command.add(""+id);
	    command.add(""+workerNumThread);
	    command.add(Boolean.toString(pipeEnable));
	    command.add("ReconfigurableNode");
	    command.add(nodeId);
	    
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
	 * @return a Process
	 * @throws IOException
	 */
	private Process startWorker(int port1, int port2, int id) throws IOException{
		List<String> command = new ArrayList<>();
		String classpath = System.getProperty("java.class.path");
	    command.add("java");
	    command.add("-Xms"+heapSize+"m");
	    command.add("-Xmx"+heapSize+"m");
	    // kill the worker on OutOfMemoryError
	    if(CRASH_ENABLED) {
              command.add("-XX:OnOutOfMemoryError="+ACTION_ON_OUT_OF_MEMORY);
            }
	    command.add("-cp");
	    command.add(classpath);
	    command.add("edu.umass.cs.gnsserver.activecode.prototype.blocking.ActiveBlockingWorker");
	    command.add(""+port1);
	    command.add(""+port2);
	    command.add(""+id);
	    command.add(""+workerNumThread);
		command.add(Boolean.toString(pipeEnable));
		command.add("ReconfigurableNode");
	    command.add(nodeId);
	    
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
	
	protected synchronized void sendMessage(ActiveMessage am){
		try {
			channel.sendMessage(am);
			ActiveCodeHandler.getLogger().log(Level.FINE, 
					"sends request:{0}", new Object[]{am});
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	static int numReq = 0;
	synchronized int incr(){
		return ++numReq;
	}
	
	/**
	 * This runCode method sends the request to worker, and
	 * wait for worker to finish the request. If the worker
	 * crashed during the request execution, this method
	 * will resend the request to a new created worker, and
	 * the new worker will execute this request again. 
	 * <p>If the worker fails to execute the request, it will 
	 * send back an error to inform this method that the execution
	 * gets accomplished with an error. This method will raise
	 * an ActiveException, and the method which calls this method
	 * needs to handle this exception.
	 * 
	 * @param guid
	 * @param accessor
	 * @param code
	 * @param value
	 * @param ttl
	 * @return executed result sent back from worker
         * @throws edu.umass.cs.gnsserver.activecode.prototype.ActiveException
	 */
	@Override
	public synchronized JSONObject runCode(InternalRequestHeader header, String guid, String accessor, 
			String code, JSONObject value, int ttl, long budget) throws ActiveException {
		
		ActiveMessage msg = new ActiveMessage(guid, accessor, code, value.toString(), ttl, budget);
		sendMessage(msg);
		
		ActiveMessage response = null;
		while( true ){
			try {
				response = (ActiveMessage) channel.receiveMessage();
			} catch (IOException e) {
				// do nothing, as the worker is crashed, the response is null
			}
			
			if(response == null){
				/**
				 *  The worker is crashed, restart the
				 *  worker.
				 */
				if(!isRestarting.getAndSet(true)){
					this.shutdown();
					this.initializeChannelAndStartWorker();
					
					isRestarting.set(false);
				}
				break;
			} else if (response.type != Type.RESPONSE){
				ActiveCodeHandler.getLogger().log(Level.FINE,
						"receive a query from worker:{0}",
						new Object[]{response});
				ActiveMessage result = queryHandler.handleQuery(response, header);
				sendMessage(result);
				ActiveCodeHandler.getLogger().log(Level.FINE,
						"send a response to worker:{0} for the query: {1}",
						new Object[]{result, response});
			} else{
				assert(response.type == Type.RESPONSE):"The message type is not RESPONSE";
				break;
			}
		}
		
		ActiveCodeHandler.getLogger().log(Level.FINE,
				"receive a response from the worker:{0}",
				new Object[]{response});
		
		if(response == null){
			throw new ActiveException("Worker crashed!");
		}
		
		if(response.getError() != null){
			throw new ActiveException(msg.toString());
		}
		counter.getAndIncrement();
		
		try {
			return new JSONObject(response.getValue());
		} catch (JSONException e) {
			return value;
		}
	}
	
        @Override
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
		final String suffix = "";
		String cfile = "/tmp/client"+suffix;
		String sfile = "/tmp/server"+suffix;
		final int numThread =2;
		
		long executionTime = 1000;
		if(System.getProperty("executionTime")!=null){
			executionTime = Long.parseLong(System.getProperty("executionTime"));
		}
		
		String codeFile = "./scripts/activeCode/noop.js";
		if(System.getProperty("codeFile")!=null){
			codeFile = System.getProperty("codeFile");
		}
		
		ActiveBlockingClient client = new ActiveBlockingClient("", null, cfile, sfile, 0, numThread);
		
		String guid = "guid";
		String field = "name";
		String code = "";
		try {
			code = new String(Files.readAllBytes(Paths.get(codeFile)));
		} catch (IOException e) {
			e.printStackTrace();
		} 
		ValuesMap value = new ValuesMap();
		value.put(field, executionTime);	
		JSONObject result = client.runCode(null, guid, field, code, value, 0, 1000);
		System.out.println(result);
		
		assertEquals(result.toString(), value.toString());
		
		int initial = client.getRecv();
		
		int n = 100000;
		
		long t1 = System.currentTimeMillis();
		
		for (int i=0; i<n; i++){
			client.runCode(null, guid, field, code, value, 0, executionTime);			
		}
		
		long elapsed = System.currentTimeMillis() - t1;
		System.out.println("It takes "+elapsed+"ms");
		System.out.println("The average time for each task is "+elapsed/n+"ms.");
		assert(client.getRecv()-initial == n):"the number of responses is not the same as the number of requests";
		
		System.out.println("Sequential throughput test succeeds.");
		
		client.shutdown();
	}
	
}
