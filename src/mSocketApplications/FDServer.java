package mSocketApplications;

//package mSocket;

import java.io.IOException;
import java.io.OutputStream;
import java.io.*;
import mSocket.*;
import java.io.*;
import java.net.*;


public class FDServer {
	public static final int TEST_PORT=3520;
	
	/* The sub-class TestServerConnection below spawns a thread
	 * for each new accepted connection.
	 */
	public static MServerSocket mss=null;
	public static class TestServerConnection implements Runnable {
		private MSocket msocket=null;
		TestServerConnection(MSocket ms) {
			msocket = ms;
		}
		public void run() {
				OutputStream out=null;
			try {
				out = msocket.getOutputStream();
                 // sendfile
                //File myFile = new File ("File_Download.tar.gz");
				//File myFile = new File ("titanium.mp3");
				File myFile = new File ("B43.pdf");
                System.out.println("file  Size "+(int)myFile.length());
                                
				//byte [] mybytearray  = new byte [(int)myFile.length()];
				byte [] mybytearray  = new byte [10000000];
				
                FileInputStream fis = new FileInputStream(myFile);
                BufferedInputStream bis = new BufferedInputStream(fis);
                boolean migrate=false;
                boolean close=false;
                int current=0;
				while( current < (int)myFile.length() )
				{
					int num=bis.read(mybytearray,0,mybytearray.length);
					int numwritten;
					/*do{
						numwritten=((MWrappedOutputStream)out).localwrite(mybytearray,0,num);
					}while(numwritten==0);*/
					((MWrappedOutputStream)out).write(mybytearray,0,num);
					numwritten=num;
					
					current+=numwritten;
					
					if( (current > ((0.5)*(int)myFile.length()) ) && migrate )
					{
						mss.migrate(InetAddress.getLocalHost(), TEST_PORT+1);
						migrate=false;
					}
					
					if( (current > ((0.7)*(int)myFile.length()) ) && close )
					{
						System.out.println("Server closed the socket");
						msocket.close();
						close=false;
						break;
					}
				}
				System.out.println("Sending Complete");
                //out.flush();
				/*while(true)
				{
					
				}*/
				//msocket.close();
			} catch(IOException e) {
				System.out.println(e); e.printStackTrace();
			}
		}
	}
	// End TestServerConnection
	
	public static void main(String[] args) {
		System.out.println("Max Heap memory"+java.lang.Runtime.getRuntime().maxMemory());
		try {
			mss = new MServerSocket(InetAddress.getByName("ananas.cs.umass.edu"),TEST_PORT);
			for(int i=0; true; i++) {
				System.out.println("Waiting for connections");
				MSocket ms = mss.accept();
				System.out.println("Accepted new connection " + (i+1));
				
                                //if(ms.isNew()) {
					FDServer.TestServerConnection tsc = new FDServer.TestServerConnection(ms);
					(new Thread(tsc)).start();
				//}
			}
			//mss.close();
		} catch(IOException e) {
			e.printStackTrace();
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}