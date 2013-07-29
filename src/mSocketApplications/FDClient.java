package mSocketApplications;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import mSocket.*;

public class FDClient {
		public static final int TEST_MIGRATE_PORT=10420;
        public static final int filesize=55939340; // filesize temporary hardcoded
        //public static final int filesize=2037361;
        //public static final int filesize=32914375;
		//public static final String ServerIP="planetlab1.inf.ethz.ch";
         public static final String ServerIP="ananas.cs.umass.edu";
        public static final int TEST_PORT=3520;
        public static int current = 0;
        public static boolean MSStart=false;
        public static boolean MSStop=false;
        public static long start=0;
        
        public static class MeasurementThread implements Runnable {
    		public void run() {
    			int lastcurr=0;
    			long time=0;
    			FileWriter fstream=null;
				try {
					fstream = new FileWriter("Results.csv");
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
    			BufferedWriter out = new BufferedWriter(fstream);
    			
    			
    			while(true)
    			{
    				if(MSStop )
    					break;	
    				try {
						Thread.sleep(100);
						time=System.currentTimeMillis();
						//time+=100;
						int localcurr=current;
						int BytesRecv=localcurr-lastcurr;
						lastcurr=localcurr;
						try {
							out.write((time-start)+","+BytesRecv+"\n");
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
    			}
    			try {
					out.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
    		}
    	}
        
        public static void main(String[] args) {
        	System.out.println("Max Heap memory"+java.lang.Runtime.getRuntime().maxMemory());
        	 start = System.currentTimeMillis();
        	 long migratestart=0,migrateend=0;
        	FDClient.MeasurementThread tsc = new FDClient.MeasurementThread();
			(new Thread(tsc)).start();
			
		try {
			
			MSocket ms = new MSocket(ServerIP, TEST_PORT);
			InetAddress iaddr = InetAddress.getByName("ananas.cs.umass.edu");
			//InetAddress iaddr = InetAddress.getByName("ananas.cs.umass.edu");
			InputStream in = ms.getInputStream();
			
                        
                       
                        int bytesRead=0;
                        //int current = 0;
                        
                        byte [] mybytearray  = new byte [10000000];
                        System.out.println("File download start 1");
                        FileOutputStream fos = new FileOutputStream("CFile_Download.tar.gz");
                        BufferedOutputStream bos = new BufferedOutputStream(fos);
                        //bytesRead = in.read(mybytearray,0,mybytearray.length);
                        //bos.write(mybytearray, 0 , bytesRead);
                        //current = bytesRead;
                        boolean migrate=true;
                        boolean closecall=false;
                        MSStart=true;
                        while( (bytesRead > -1) && current <  filesize ) {
                        	if(current> (0.5*filesize) && migrate)
                        	{
                        		System.out.println("Current "+current);
                        		//Thread.sleep(60000);
                        		migratestart=System.currentTimeMillis();
                        		ms.migrateLocal(iaddr, TEST_MIGRATE_PORT);
                        		migrateend=System.currentTimeMillis();
                    			System.out.println("Completed client-initiated migration");
                    			migrate=false;
                        	}
                        	if( !migrate && current > (0.7*filesize) && closecall )
                        	{
                        		System.out.println("Client doesn't want to listen anymore closes the socket");
                        		
                        		ms.close();
                        		closecall=false;
                        		break;
                        	}
                        	
                           bytesRead =
                              in.read(mybytearray, 0, mybytearray.length);
                           System.out.println("bytesread "+bytesRead);
                           
                           if(bytesRead >= 0) {
                        	   bos.write(mybytearray, 0 , bytesRead);
                        	   current += bytesRead;
                           }
                           System.out.println("Current pos "+current);
                           bos.flush();
                        }
                        bos.flush();
                        System.out.println("File download complete");
                        MSStop=true;
 
                        long end = System.currentTimeMillis();
                        System.out.println("Download time "+ (end-start)+ " Migration time "+(migrateend-migratestart) +" Migration start "+(migratestart-start) +" Migration end "+(migrateend-start));
                        bos.close();
                        
                        //ms.close();
			/*ms.migrateLocal(iaddr, TEST_MIGRATE_PORT);
			System.out.println("Completed client-initiated migration");*/
		
		//	ms.close();
		} catch(IOException e) {
			System.out.println("TestClient: " + e);
			e.printStackTrace();
		} catch(Exception e) {
			System.out.println("TestClient: " + e);
			e.printStackTrace();
		}
	}
}