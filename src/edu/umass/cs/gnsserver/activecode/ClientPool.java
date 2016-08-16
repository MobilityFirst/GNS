/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *  Initial developer(s): Misha Badov, Westy
 *
 */
package edu.umass.cs.gnsserver.activecode;

import java.io.File;
import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;

import edu.umass.cs.gnsserver.activecode.protocol.ActiveCodeMessage;
import edu.umass.cs.gnsserver.interfaces.ActiveDBInterface;
import edu.umass.cs.gnsserver.gnsapp.deprecated.GNSApplicationInterface;

/**
 * This class represents a pool of active code clients. Each client is associated with a particular thread.
 * This is necessary because of limitations of Java's ThreadPoolExecutor.
 *
 * @author mbadov
 *
 */

public class ClientPool implements Runnable{	
	private HashMap<Long, ActiveCodeClient> clients;
	private ActiveDBInterface app;
	private ConcurrentHashMap<Integer, Process> spareWorkers;
	
	/**
	 * Maintain the map from a port of worker to an ActiveCodeClient
	 * When an ActiveCodeClient's corresponding worker is shutdown,
	 * this map should be updated accordingly.
	 * This client pool should be able to retrieve the ActiveCodeClient
	 * based on the worker's port number.
	 */
	private final ConcurrentHashMap<Integer, ActiveCodeClient> workerPortToClient;
	private final ConcurrentHashMap<Integer, Boolean> portStatus;
	//private final ConcurrentHashMap<Integer, Long> startTime = new ConcurrentHashMap<Integer, Long>();
	
	//private static ConcurrentHashMap<Integer, Long> timeMap = new ConcurrentHashMap<Integer, Long>();
	private ExecutorService executorPool;
	private final int numSpareWorker = 1; //AppReconfigurableNodeOptions.activeCodeSpareWorker;
	
	private static DatagramSocket tempSocket;
	
	public final static int CALLBACK_PORT = 60000;
	
	private static int clientID = 0;
	static synchronized int getClientID(){
		return ++clientID;
	}

	
	/**
	 * Initialize a ClientPool
	 * @param app 
	 */
	public ClientPool(ActiveDBInterface app) {
		try{
			tempSocket = new DatagramSocket();
			tempSocket.close();
		}catch(IOException e){
			e.printStackTrace();
		}
		
		clients = new HashMap<>();
		this.app = app;
		spareWorkers = new ConcurrentHashMap<Integer, Process>();
		
		// Initialize the worker port to client map
		workerPortToClient = new ConcurrentHashMap<Integer, ActiveCodeClient>();
		portStatus = new ConcurrentHashMap<Integer, Boolean>();
		
		executorPool = Executors.newFixedThreadPool(numSpareWorker);
		System.out.println("Starting "+ActiveCodeConfig.activeCodeWorkerCount+
				" workers with "+ActiveCodeConfig.activeCodeSpareWorker+" spare workers");
	}
	
	public void run(){
		DatagramSocket socket = null;
		try{
			socket = new DatagramSocket(CALLBACK_PORT);
		}catch(IOException e){
			e.printStackTrace();
		}
		boolean keepGoing = true;
		
		while(keepGoing){
			byte[] buffer = new byte[2048];
    		DatagramPacket pkt = new DatagramPacket(buffer, buffer.length);
    		try{
    			socket.receive(pkt);
    			int workerPort = pkt.getPort();
    			
    			/*
    			 * Invariant: the worker port must already exist, and its value should be false.
    			 * Don't put it into an assert
    			 */
    			boolean existed = setPortStatus(workerPort, true);
    			assert(existed == false);
    			ActiveCodeClient client = workerPortToClient.get(workerPort);
    			// if client is null, it means that it's a spare worker
    			if(client != null){
    				// If client is not null, then it's not a spare client, it's an ActiveCodeClient
    				client.setReady(true);
    			}
    		}catch(IOException e){
    			e.printStackTrace();
    			keepGoing = false;
    		}
		}		
		socket.close();
	}
	
	/**
	 * Thread factory uses this method to map a thread to an ActiveCodeClient
	 * @param t
	 */
	protected ActiveCodeClient addClient(Thread t) {
		/*
		 * Invariant: after the client starts, no new thread and client should be created.
		 */
		assert(getClientID() <= ActiveCodeConfig.activeCodeWorkerCount);
		
		System.out.println("Add a client for thread "+t);
		int workerPort = getOpenUDPPort();
		
		// This port is not ready
		setPortStatus(workerPort, false);
		
		Process proc = startNewWorker(workerPort, 64);
		ActiveCodeClient client = new ActiveCodeClient(app, workerPort, proc);
		clients.put(t.getId(), client);
		workerPortToClient.put(workerPort, client);
		
		while(!client.isReady()){
			synchronized(client){
				try{
					client.wait();
				} catch(InterruptedException e){
					e.printStackTrace();
				}
			}
		}
		
		return client;
	}
	
	/**
	 * The getter for fetching an ActiveCodeClient through its thread id
	 * @param tid
	 * @return an ActiveCodeClient
	 */
	public ActiveCodeClient getClient(long tid) {
		return clients.get(tid);
	}
	
	/**
	 * grab a port that is available at this point
	 * @return an available port number
	 */
	protected static int getOpenUDPPort() {
		int port = 0;
		synchronized(tempSocket){
			try{
				tempSocket = new DatagramSocket(0);
				port = tempSocket.getLocalPort();
				tempSocket.close();
			} catch(IOException e) {
				e.printStackTrace();
			}
		}
		return port;
	}
	
	protected void startSpareWorkers(){
		for (int i=0; i<numSpareWorker; i++){
			executorPool.execute(new WorkerGeneratorRunanble());
		}
	}
	
	/**
	 * Shutdown all the workers
	 */
	public void shutdown() {
		System.out.println(this.getClass().getSimpleName()+":close all workers!");
		for(ActiveCodeClient client : clients.values()) {
		    client.shutdownServer();
		}
		
		DatagramSocket socket = null;
		try{
			socket = new DatagramSocket();
		} catch(IOException e){
			e.printStackTrace();
		}
		
		for (int port : spareWorkers.keySet()){
			ActiveCodeMessage acm = new ActiveCodeMessage();
			acm.setShutdown(true);
			try {
				// One message for setting socket number, another for shutdown the worker
				ActiveCodeUtils.sendMessage(socket, acm, port);
				ActiveCodeUtils.sendMessage(socket, acm, port);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		socket.close();
	}
	
	protected synchronized void updatePortToClientMap(int oldPort, int newPort, ActiveCodeClient client){
		/*
		 * Invariant: port and client pair must always exist, otherwise we will lose a worker
		 */
		boolean portExists = workerPortToClient.remove(oldPort, client);
		if (!portExists){
			System.out.println(workerPortToClient+ " \n The port to be removed is "+oldPort+" "+newPort+" "+client);
			assert(portExists);
		}
		workerPortToClient.put(newPort, client);
	}
	
	
	protected int getSpareWorkerPort(){	
		int count = 0;
		while(spareWorkers.keySet().isEmpty()){
			System.out.println("The spare worker set is empty "+(++count));
			synchronized(spareWorkers){
				try{
					spareWorkers.wait();
				} catch(InterruptedException ie) {
					ie.printStackTrace();
				}
			}
		}
		/*
		 * Invariant: the spare worker set should not be empty
		 */
		assert(!spareWorkers.keySet().isEmpty());
		int port = spareWorkers.keys().nextElement();
		return port;
	}
	
	protected synchronized Process getSpareWorker(int port){
		return spareWorkers.remove(port);
	}
	
	protected synchronized boolean getPortStatus(int port){
		return portStatus.get(port);
	}
	
	protected synchronized Boolean setPortStatus(int port, boolean status){
		return portStatus.put(port, status);
	}
	
	protected Process startNewWorker(int workerPort, int initMemory){
		List<String> command = new ArrayList<String>();
		// Get the current classpath
		// client port is unknown in the beginning for the worker
		String classpath = System.getProperty("java.class.path");
	    command.add("java");
	    command.add("-Xms"+initMemory+"m");
	    command.add("-Xmx64m");
	    command.add("-cp");
	    command.add(classpath);
	    command.add("edu.umass.cs.gnsserver.activecode.worker.ActiveCodeWorker");
	    command.add(Integer.toString(workerPort));
	    
	    ProcessBuilder builder = new ProcessBuilder(command);
		builder.directory(new File(System.getProperty("user.dir")));
		
		builder.redirectError(Redirect.INHERIT);
		builder.redirectOutput(Redirect.INHERIT);
		builder.redirectInput(Redirect.INHERIT);
		
		Process process = null;
		try{
			process = builder.start();
		}catch(IOException e){
			e.printStackTrace();
		}
		
		return process;
	}
	
	/**
	 * Start a spare worker
	 */
	public synchronized void addSpareWorker(){
		int workerPort = getOpenUDPPort();	
		setPortStatus(workerPort, false);
		if(ActiveCodeHandler.enableDebugging)
			ActiveCodeHandler.getLogger().log(Level.INFO, "ActiveCodeClient:"+workerPort+" is being started.");
		
		Process process = startNewWorker(workerPort, 16);
		
		spareWorkers.put(workerPort, process);
		synchronized(spareWorkers){
			spareWorkers.notify();
		}
	}
	
	protected void generateNewWorker(){		
		executorPool.execute(new WorkerGeneratorRunanble());
	}

	private class WorkerGeneratorRunanble implements Runnable{		
		public void run(){			
			addSpareWorker();
		}
	}

}
