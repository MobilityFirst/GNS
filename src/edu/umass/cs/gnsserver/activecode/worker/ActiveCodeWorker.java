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
package edu.umass.cs.gnsserver.activecode.worker;

import java.io.IOException;
import java.net.DatagramSocket;

import edu.umass.cs.gnsserver.activecode.ActiveCodeUtils;
import edu.umass.cs.gnsserver.activecode.protocol.ActiveCodeMessage;
import edu.umass.cs.utils.DelayProfiler;

/**
 * This class is the worker to run active code
 * in an isolated process.
 * 
 * @author Zhaoyu Gao
 */
public class ActiveCodeWorker {
	
	protected static int numReqs = 0;
	private DatagramSocket serverSocket;
	
	protected ActiveCodeWorker(int port) {
		try{
			this.serverSocket = new DatagramSocket(port);
			System.out.println(this+" : Starting ActiveCodeWorker at port number " + port);
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	
	public String toString(){
		return "ActiveCodeWorker"+serverSocket.getLocalPort();
	}
	
	/**
	 * Starts the worker listener 
	 * @throws IOException
	 */
	public void run() throws IOException {	
        ActiveCodeRunner runner = new ActiveCodeRunner();
        
    	RequestHandler handler = new RequestHandler(runner);
    	boolean keepGoing = true;

		// Notify the server that I'm ready
    	
		ActiveCodeUtils.sendMessage(serverSocket, new ActiveCodeMessage(), 60000);
		
        while (keepGoing) {        	
        	keepGoing = handler.handleRequest(serverSocket);        	
        	if(numReqs%1000 == 0){
        		System.out.println(DelayProfiler.getStats());
        	}
        	numReqs++;
        }
        
        serverSocket.close();
	}
	
	/**
	 * Test single worker
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException  {
		
		int port = 0;
		port = Integer.parseInt(args[0]);
		
		ActiveCodeWorker acs = new ActiveCodeWorker(port);
		acs.run();
		
    }
}
