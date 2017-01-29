package edu.umass.cs.gnsserver.activecode.prototype.unblocking;


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
import edu.umass.cs.utils.DelayProfiler;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;


public class ActiveNonBlockingClient implements Runnable,Client {
	
	private final static int DEFAULT_HEAP_SIZE = ActiveCodeConfig.activeWorkerHeapSize;
	private final static String actionOnOutOfMemory = "kill -9 %p";
	
	private ActiveQueryHandler queryHandler;
	private final String nodeId;
	private Channel channel;
	private final String ifile;
	private final String ofile;
	private final int workerNumThread;
	
	private ConcurrentHashMap<Long, Monitor> tasks = new ConcurrentHashMap<Long, Monitor>();
	
	private Process workerProc;
	final private int id;
	final private boolean pipeEnable;
	final private boolean crashEnabled = ActiveCodeConfig.activeCrashEnabled;
	
	private final int heapSize;
	
	private static long lastWorkerStartedTime;
	private AtomicBoolean isRestarting = new AtomicBoolean();
	


	public Process getWorker(){
		return workerProc;
	}
	
	private AtomicInteger counter = new AtomicInteger(0);
	

	public int getRecv(){
		return counter.get();
	}
	

	public ActiveNonBlockingClient(String nodeId, ActiveDBInterface app, String ifile, String ofile, int id, int workerNumThread, int heapSize){
		this.nodeId = nodeId;
		this.id = id;
		this.ifile = ifile;
		this.ofile = ofile;
		this.pipeEnable = true;
		this.workerNumThread = workerNumThread;
		this.heapSize = heapSize;
		
		queryHandler = new ActiveQueryHandler(app);
		
		lastWorkerStartedTime = System.currentTimeMillis();
		initializeChannelAndStartWorker();
		
		
	}
	

	public ActiveNonBlockingClient(String nodeId, ActiveDBInterface app, String ifile, String ofile, int id, int workerNumThread){
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
		DelayProfiler.updateDelay("activeRestartWorker", lastWorkerStartedTime);
	}
	

	public ActiveNonBlockingClient(String nodeId, ActiveDBInterface app, int port, int serverPort, int id, int workerNumThread){
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
	

	public ActiveNonBlockingClient(String nodeId, ActiveDBInterface app, String ifile, String ofile){
		this(nodeId, app, ifile, ofile, 0, 1);
	}
	
	@Override
	public void run() {

		
		while(!Thread.currentThread().isInterrupted()){
			ActiveMessage response;
			try {
				if( (response = (ActiveMessage) channel.receiveMessage()) != null){					
					long id = response.getId();
					Monitor monitor = tasks.get(id);
					assert(monitor != null):"the corresponding monitor is null!";
					ActiveCodeHandler.getLogger().log(ActiveCodeHandler.DEBUG_LEVEL,
							"receive a result or query from the worker:{0}",
							new Object[]{response});
					
					monitor.setResult(response, response.type == Type.RESPONSE);
				} else {
					if(!isRestarting.getAndSet(true)){
						lastWorkerStartedTime = System.currentTimeMillis();
						// restart the worker
						this.shutdown();
						this.initializeChannelAndStartWorker();
						
						// release all the requests that waited on its monitor
						for(Monitor monitor:this.tasks.values()){
							monitor.setResult(null, true);
						}
						isRestarting.set(false);
						
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	

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
	

	private Process startWorker(String ifile, String ofile, int id) throws IOException{
		List<String> command = new ArrayList<String>();
		String classpath = System.getProperty("java.class.path");
	    command.add("java");
	    command.add("-Xms"+heapSize+"m");
	    command.add("-Xmx"+heapSize+"m");
	    // kill the worker on OutOfMemoryError
	    if(crashEnabled)
	    	command.add("-XX:OnOutOfMemoryError="+actionOnOutOfMemory);
	    command.add("-cp");
	    command.add(classpath);
	    command.add("edu.umass.cs.gnsserver.activecode.prototype.unblocking.ActiveNonBlockingWorker");
	    command.add(ifile);
	    command.add(ofile);
	    command.add(""+id);
	    command.add(""+workerNumThread);
	    command.add(ActiveCodeConfig.activeGeoIPFilePath);
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
	

	private Process startWorker(int port1, int port2, int id) throws IOException{
		List<String> command = new ArrayList<String>();
		String classpath = System.getProperty("java.class.path");
	    command.add("java");
	    command.add("-Xms"+heapSize+"m");
	    command.add("-Xmx"+heapSize+"m");
	    // kill the worker on OutOfMemoryError
	    if(crashEnabled)
	    	command.add("-XX:OnOutOfMemoryError="+actionOnOutOfMemory);
	    command.add("-cp");
	    command.add(classpath);
	    command.add("edu.umass.cs.gnsserver.activecode.prototype.unblocking.ActiveNonBlockingWorker");
	    command.add(""+port1);
	    command.add(""+port2);
	    command.add(""+id);
	    command.add(""+workerNumThread);
	    command.add(ActiveCodeConfig.activeGeoIPFilePath);
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
			ActiveCodeHandler.getLogger().log(ActiveCodeHandler.DEBUG_LEVEL, 
					"sends request:{0}", new Object[]{am});
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	static int numReq = 0;
	synchronized int incr(){
		return ++numReq;
	}
	

	@Override
	public JSONObject runCode(InternalRequestHeader header, String guid, String accessor, 
			String code, JSONObject valuesMap, int ttl, long budget) throws ActiveException {
		
		ActiveMessage msg = new ActiveMessage(guid, accessor, code, valuesMap.toString(), ttl, budget);
		Monitor monitor = new Monitor();
		tasks.put(msg.getId(), monitor);
		
		ActiveMessage response = null;
		synchronized(monitor){
			while( !monitor.getDone() ){				
				try {
					if(!monitor.getWait()){
						sendMessage(msg);
						monitor.setWait();
					}					
					monitor.wait();
				} catch (InterruptedException e) {
					// this thread is interrupted, do nothing
					e.printStackTrace();
				}				
				
				response = monitor.getResult();	
				

				if (response != null && response.type != Type.RESPONSE){
					// submit the task to the worker and wait for the response
					queryHandler.handleQueryAsync(response, header, monitor);
					try {
						monitor.wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}

					
					if(!monitor.getDone()){
						// If it's not because of the timeout event, then send back the response  
						ActiveMessage result = monitor.getResult();	
						sendMessage(result);
					}else{
						// If it's because of timeout, then clean up the state and exit the loop
						break;
					}
					
				}
			}		
		}
		
		response = monitor.getResult();
		
		ActiveCodeHandler.getLogger().log(ActiveCodeHandler.DEBUG_LEVEL,
				"receive a response from the worker:{0}",
				new Object[]{response});
		
		if(response == null){

			throw new ActiveException("Worker crashes!");
		}
		if(response.getError() != null){
			throw new ActiveException("Message: " + msg.toString() +
                                " Response: " + response.toString());
		}
		counter.getAndIncrement();
		tasks.remove(response.getId());
		
		try {
			return new JSONObject(response.getValue());
		} catch (JSONException e) {
			throw new ActiveException("Bad JSON value returned from active code!");
		}
	}
	
	public String toString(){
		return this.getClass().getSimpleName()+id;
	}
	

	public static class Monitor {
		boolean isDone;
		ActiveMessage response;
		boolean waited;
		
		Monitor(){
			this.isDone = false;
			this.waited = false;
		}
		

		public boolean getDone(){
			return isDone;
		}
		

		public synchronized void setResult(ActiveMessage response, boolean isDone){
			this.response = response;
			this.isDone = isDone;
			notifyAll();
		}
		
		ActiveMessage getResult(){
			return response;
		}
		
		void setWait(){
			waited = true;
		}
		
		boolean getWait(){
			return waited;
		}
	}
	

	public static void main(String[] args) throws InterruptedException, JSONException, ActiveException{
		
	}
	
}
