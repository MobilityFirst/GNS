/*
 * Copyright (C) 2015
 * University of Massachusetts
 * All Rights Reserved 
 *
 */
package edu.umass.cs.gns.activecode.worker;

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
