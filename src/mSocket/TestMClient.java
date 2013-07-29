package mSocket;


import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;

/*public class TestMClient {
	public static final int TEST_MIGRATE_PORT=10420;
	public static void main(String[] args) {
		try {
			MSocket ms = new MSocket("localhost", TestMServer.TEST_PORT);
			InetAddress iaddr = (InetAddress.getLocalHost());
			InputStream in = ms.getInputStream();
			OutputStream out = ms.getOutputStream();
			for(int i=0; i<4; i++) {
				byte[] b = new byte[32];
				in.read(b);
				System.out.println(new String(b));
				out.write("From the client side".getBytes());
				out.write(" comes back a response".getBytes());
			}
			ms.migrateLocal(iaddr, TEST_MIGRATE_PORT);
			System.out.println("Completed client-initiated migration");
			for(int i=0; i<10; i++) {
				byte[] b = new byte[48];
				in.read(b);
				System.out.println(new String(b));
				out.write("From the client side".getBytes());
				out.write(" comes back a response".getBytes());
			}
			ms.close();
		} catch(IOException e) {
			System.out.println("TestClient: " + e);
			e.printStackTrace();
		} catch(Exception e) {
			System.out.println("TestClient: " + e);
			e.printStackTrace();
		}
	}
}*/


public class TestMClient {
	public static final int TEST_MIGRATE_PORT=10420;
	public static void main(String[] args) {
		try {
			MSocket ms = new MSocket("localhost", TestMServer.TEST_PORT);
			InetAddress iaddr = (InetAddress.getByName("localhost"));
			InputStream in = ms.getInputStream();
			OutputStream out = ms.getOutputStream();
			/*for(int i=0; i<4; i++) {
				byte[] b = new byte[32];
				in.read(b);
				System.out.println(new String(b));*/
				//out.write("From the client side 1".getBytes());
				//out.write(" comes back a response 1".getBytes());
			//}
			ms.migrateLocal(iaddr, TEST_MIGRATE_PORT);
			System.out.println("Completed client-initiated migration");
			
			//for(int i=0; i<10; i++) {
				//byte[] b = new byte[48];
				//in.read(b);
				//System.out.println(new String(b));
				out.write("From the client side 2 comes back a response 3".getBytes());
				out.write("comes back a response 2".getBytes());
				//out.write("From the client side 2".getBytes());
				//out.write(" comes back a response 2".getBytes());
			//}
			ms.close();
		}  catch(Exception e) {
			System.out.println("TestClient: " + e);
			e.printStackTrace();
		}
	}
}