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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import edu.umass.cs.gnsserver.gnsApp.GnsApplicationInterface;

/**
 * This class represents a pool of active code clients. Each client is associated with a particular thread.
 * This is necessary because of limitations of Java's ThreadPoolExecutor.
 * @author mbadov
 *
 */
public class ClientPool {
	Map<Long, ActiveCodeClient> clients;
	GnsApplicationInterface<String> app;
	ActiveCodeHandler ach;
	ConcurrentHashMap<Integer, Process> spareWorkers;
	private ExecutorService executorPool;
	
	/**
	 * Initialize a ClientPool
	 * @param app
	 */
	public ClientPool(GnsApplicationInterface<String> app, ActiveCodeHandler ach) {
		clients = new HashMap<>();
		this.app = app;
		this.ach = ach;
		spareWorkers = new ConcurrentHashMap<Integer, Process>();
		executorPool = Executors.newFixedThreadPool(5);
		for (int i=0; i<0; i++){
			executorPool.execute(new WorkerGeneratorRunanble());
		}
	}
	
	protected void addClient(Thread t) {
		clients.put(t.getId(), new ActiveCodeClient(app, -1, ach));
	}
	
	protected void addClient(Thread t, int port){
		clients.put(t.getId(), new ActiveCodeClient(app, port, ach));
	}
	
	protected ActiveCodeClient getClient(long pid) {
		return clients.get(pid);
	}
	
	protected void shutdown() {
		for(ActiveCodeClient client : clients.values()) {
		    client.shutdownServer();
		}
	}
	
	protected int getSpareWorkerPort(){
		int port = spareWorkers.keys().nextElement();
		return port;
	}
	
	protected Process getSpareWorker(int port){
		while(spareWorkers.size() == 0){
			synchronized(spareWorkers){
				try{
					spareWorkers.wait();
				}catch(InterruptedException e){
					e.printStackTrace();
				}
			}
		}
		return spareWorkers.remove(port);
	}
	
	protected void addSpareWorker(){
		List<String> command = new ArrayList<>();
		int serverPort = ActiveCodeClient.getOpenUDPPort();

		// Get the current classpath
		String classpath = System.getProperty("java.class.path");
				
	    command.add("java");
	    command.add("-Xms16m");
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
		
		spareWorkers.put(serverPort, process);
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
