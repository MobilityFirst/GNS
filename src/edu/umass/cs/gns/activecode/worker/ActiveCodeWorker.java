package edu.umass.cs.gns.activecode.worker;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import edu.umass.cs.gns.database.MongoRecords;
import edu.umass.cs.gns.nsdesign.Config;

public class ActiveCodeWorker {
	public void run(int port) throws IOException {	
        ServerSocket listener = new ServerSocket(port);
        ActiveCodeRunner runner = new ActiveCodeRunner();
        
        System.out.println("Starting ActiveCode Server at " + 
        		listener.getInetAddress().getCanonicalHostName() + 
        		":" + listener.getLocalPort());
        
        try {
        	RequestHandler handler = new RequestHandler(runner);
        	boolean keepGoing = true;
        	
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
		int port = 0;
		
		if(args.length == 1) {
			port = Integer.parseInt(args[0]);
		}		
		
		acs.run(port);
    }
}
