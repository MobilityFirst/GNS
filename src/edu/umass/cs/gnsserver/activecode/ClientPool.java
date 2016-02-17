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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import edu.umass.cs.gnsserver.activecode.protocol.ActiveCodeMessage;
import edu.umass.cs.gnsserver.gnsApp.AppReconfigurableNodeOptions;
import edu.umass.cs.gnsserver.gnsApp.GnsApplicationInterface;
import edu.umass.cs.utils.DelayProfiler;

/**
 * This class represents a pool of active code clients. Each client is associated with a particular thread.
 * This is necessary because of limitations of Java's ThreadPoolExecutor.
 * @author mbadov
 *
 */
public class ClientPool implements Runnable{
	private Map<Long, ActiveCodeClient> clients;
	private GnsApplicationInterface<String> app;
	private ActiveCodeHandler ach;
	private ConcurrentHashMap<Integer, Process> spareWorkers;
	private static ConcurrentHashMap<Integer, Long> timeMap = new ConcurrentHashMap<Integer, Long>();
	private ExecutorService executorPool;
	private final int numSpareWorker = AppReconfigurableNodeOptions.activeCodeSpareWorker;
	private Lock lock = new ReentrantLock();
	
	private final static int CALLBACK_PORT = 60000;
	/**
	 * Maintain the state of all available ports, including active and spare
	 */
	private static ConcurrentHashMap<Integer, Boolean> readyMap;
	
	
	/**
	 * Initialize a ClientPool
	 * @param app
	 */
	public ClientPool(GnsApplicationInterface<String> app, ActiveCodeHandler ach) {
		clients = new HashMap<>();
		this.app = app;
		this.ach = ach;
		spareWorkers = new ConcurrentHashMap<Integer, Process>();
		readyMap = new ConcurrentHashMap<Integer, Boolean>();
		executorPool = Executors.newFixedThreadPool(5);
		System.out.println("Starting "+AppReconfigurableNodeOptions.activeCodeWorkerCount+
				" workers with "+AppReconfigurableNodeOptions.activeCodeSpareWorker+" spare workers");
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
			byte[] buffer = new byte[1024];
    		DatagramPacket pkt = new DatagramPacket(buffer, buffer.length);
    		try{
    			socket.receive(pkt);
    			int clientPort = pkt.getPort();
    			updateClientState(clientPort, true);
    			synchronized(lock){
    				lock.notifyAll();
    			}
    		}catch(IOException e){
    			e.printStackTrace();
    			keepGoing = false;
    		}
		}		
		socket.close();
	}
	
	protected static int getOpenUDPPort() {
		int port = 0;
		try{
			DatagramSocket serverSocket = new DatagramSocket(0);
			port = serverSocket.getLocalPort();
			serverSocket.close();
		} catch(IOException e) {
			e.printStackTrace();
		}
		return port;
	}
	
	protected void startSpareWorkers(){
		for (int i=0; i<numSpareWorker; i++){
			executorPool.execute(new WorkerGeneratorRunanble());
		}
	}
	
	protected void waitFor(){
		synchronized(lock){
			try{
				lock.wait();
			}catch(InterruptedException e){
				e.printStackTrace();
			}
		}
	}
	
	protected static void removeClientState(int port){
		readyMap.remove(port);
	}
	
	protected static void updateClientState(int port, boolean state){
		// update the state of the worker
		readyMap.put(port, state);	
		if(state){
			DelayProfiler.updateDelay("activeStartNewWorker", timeMap.get(port));
		}else{
			timeMap.put(port, System.currentTimeMillis());
		}
	}
	
	protected static boolean getClientState(int port){
		return readyMap.get(port);
	}
	
	protected void addClient(Thread t) {
		int serverPort = getOpenUDPPort();
		updateClientState(serverPort, false);
		
		Process proc = startNewWorker(serverPort, 64);
		clients.put(t.getId(), new ActiveCodeClient(app, ach, this, serverPort, proc));
	}
	
	protected ActiveCodeClient getClient(long pid) {
		return clients.get(pid);
	}
	
	protected void shutdown() {
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
				ActiveCodeUtils.sendMessage(socket, acm, port);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		socket.close();
	}
	
	
	protected int getSpareWorkerPort(){
		while(spareWorkers.keySet().isEmpty()){
			System.out.println("The spare worker set is empty.");
			synchronized(spareWorkers){
				try{
					spareWorkers.wait();
				} catch(InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		//assert(spareWorkers.keySet().isEmpty());
		int port = spareWorkers.keys().nextElement();
		//System.out.println("The port is "+port);
		return port;
	}
	
	protected Process getSpareWorker(int port){
		return spareWorkers.remove(port);
	}
	
	protected Process startNewWorker(int serverPort, int initMemory){
		List<String> command = new ArrayList<>();
		// Get the current classpath
		String classpath = System.getProperty("java.class.path");
	    command.add("java");
	    command.add("-Xms"+initMemory+"m");
	    command.add("-Xmx64m");
	    command.add("-cp");
	    command.add(classpath);
	    command.add("edu.umass.cs.gnsserver.activecode.worker.ActiveCodeWorker");
	    command.add(Integer.toString(serverPort));
	    command.add(Integer.toString(-1));
	    command.add(Integer.toString(-1));
	    
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
	
	protected void addSpareWorker(){
		int serverPort = getOpenUDPPort();
		// maintain state for a worker
		updateClientState(serverPort, false);		
		
		Process process = startNewWorker(serverPort, 64);
		
		spareWorkers.put(serverPort, process);
		synchronized(spareWorkers){
			spareWorkers.notifyAll();
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
