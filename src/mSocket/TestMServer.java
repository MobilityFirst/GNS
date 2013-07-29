package mSocket;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

public class TestMServer {
	public static final int TEST_PORT=3520;
	
	/* The sub-class TestServerConnection below spawns a thread
	 * for each new accepted connection.
	 */
	public static class TestServerConnection implements Runnable {
		private MSocket msocket=null;
		TestServerConnection(MSocket ms) {
			msocket = ms;
		}
		public void run() {
			try {
				OutputStream out = msocket.getOutputStream();
				InputStream in = msocket.getInputStream();
				for(int i=0; i<10; i++) {
					//out.write("Testing the waters".getBytes());
					//out.write(" to get a feel".getBytes());
					String str="";
					int c=0;
					//do{
					byte[] buf = new byte[100];
					c=in.read(buf);
					str=new String(buf);
					//}while(c <2);
					if(str.length()>3)
					System.out.println(str);
					//Thread.sleep(1000);
				}
			} catch(IOException  e) {
				System.out.println(e); e.printStackTrace();
			} 
		}
	}
	// End TestServerConnection
	
	public static void main(String[] args) {
		try {
			MServerSocket mss = new MServerSocket(InetAddress.getByName("localhost"),TEST_PORT);
			for(int i=0; true; i++) {
				MSocket ms = mss.accept();
				System.out.println("Accepted new connection " + (i+1));
				//if(ms.isNew()) {
					TestServerConnection tsc = new TestServerConnection(ms);
					(new Thread(tsc)).start();
				//}
				/*if(i==1) {
					Thread.sleep(2000);
					mss.migrate(InetAddress.getLocalHost(), TEST_PORT+1);
				}
				if(i==2)
					break;*/
			}
			//mss.close();
		} catch(IOException e) {
			e.printStackTrace();
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}