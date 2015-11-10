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
import java.net.ServerSocket;
import java.net.Socket;

public class ActiveCodeWorker {
	/**
	 * Starts the worker listener
	 * @param port the port
	 * @param callbackPort the port at which to ping the GNS to signal the ready state
	 * @throws IOException
	 */
	public void run(int port, int callbackPort) throws IOException {	
        ServerSocket listener = new ServerSocket(port);
        ActiveCodeRunner runner = new ActiveCodeRunner();
        
        System.out.println("Starting ActiveCode Server at " + 
        		listener.getInetAddress().getCanonicalHostName() + 
        		":" + listener.getLocalPort());
        
        try {
        	RequestHandler handler = new RequestHandler(runner);
        	boolean keepGoing = true;
        	
        	// Notify the server that we are ready
        	Socket temp = new Socket("0.0.0.0", callbackPort);
        	temp.close();
        	
            while (keepGoing) {
            	Socket s = listener.accept();
            	keepGoing = handler.handleRequest(s);
            	s.close();
            }
        } catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
            listener.close();
        }
	}
	
	public static void main(String[] args) throws IOException  {
		ActiveCodeWorker acs = new ActiveCodeWorker();	
		int port = 0, callbackPort = 0;
		
		if(args.length == 2) {
			port = Integer.parseInt(args[0]);
			callbackPort = Integer.parseInt(args[1]);
		}	
		
		acs.run(port, callbackPort);
    }
}
