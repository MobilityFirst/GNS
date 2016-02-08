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
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.Socket;

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
	
	private static int numReqs = 0;
	private DatagramSocket serverSocket;
	private int clientPort = -1;
	
	public ActiveCodeWorker(int port, int callbackPort) {
		try{
			this.serverSocket = new DatagramSocket(port);
			System.out.println("Starting ActiveCodeWorker at port number " + port);
			this.clientPort = callbackPort;
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	
	/**
	 * Starts the worker listener
	 * @throws IOException
	 */
	public void run(int readyPort) throws IOException {	
        ActiveCodeRunner runner = new ActiveCodeRunner();
        
    	RequestHandler handler = new RequestHandler(runner, this.clientPort);
    	boolean keepGoing = true;

		// Notify the server that we are ready
		if (readyPort != -1){
			Socket temp = new Socket("0.0.0.0", readyPort);
			temp.close();
		} else {
			//System.out.println("Notify clientPool through port "+serverSocket.getLocalPort());
			ActiveCodeUtils.sendMessage(serverSocket, new ActiveCodeMessage(), 60000);
		}
		
        while (keepGoing) {
        	if (clientPort == -1){
        		byte[] buffer = new byte[1024];
        		DatagramPacket pkt = new DatagramPacket(buffer, buffer.length);
        		try{
        			serverSocket.receive(pkt);
        			clientPort = pkt.getPort();
        			handler.setPort(clientPort);
        		}catch(IOException e){
        			e.printStackTrace();
        		}
        		continue;
        	}
        	keepGoing = handler.handleRequest(serverSocket);
        	numReqs++;
        	if(numReqs%1000 == 0){
        		System.out.println(DelayProfiler.getStats());
        	}
        }
        
        serverSocket.close();
	}
	
	/**
	 * Test single worker
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException  {
		
		int port = 0, callbackPort = -1, readyPort = -1;
		port = Integer.parseInt(args[0]);
		
		if(args.length == 3) {	
			callbackPort = Integer.parseInt(args[1]);
			readyPort = Integer.parseInt(args[2]);
			ActiveCodeWorker acs = new ActiveCodeWorker(port, callbackPort);
			acs.run(readyPort);
		}		
    }
}
