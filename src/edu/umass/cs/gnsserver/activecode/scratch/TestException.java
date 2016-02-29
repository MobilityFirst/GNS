package edu.umass.cs.gnsserver.activecode.scratch;

import java.net.DatagramSocket;
import java.net.SocketException;

import edu.umass.cs.gnsserver.activecode.ActiveCodeUtils;
import edu.umass.cs.gnsserver.activecode.protocol.ActiveCodeMessage;

public class TestException {
	
	static class CloseSocket implements Runnable{
		private DatagramSocket socket;
		
		public CloseSocket(DatagramSocket socket) {
			// TODO Auto-generated constructor stub
			this.socket = socket;
		}

		@Override
		public void run() {
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			socket.close();
			System.out.println("Socket closed");
		}
		
	}
	
	public static void main(String[] args){
		
		DatagramSocket socket1 = null;
		DatagramSocket socket2 = null;
		try {
			socket1 = new DatagramSocket();
			socket2 = new DatagramSocket();
		} catch (SocketException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		new Thread(new CloseSocket(socket1)).start();
		new Thread(new CloseSocket(socket2)).start();
		while(true){
			ActiveCodeMessage acm = ActiveCodeUtils.receiveMessage(socket2, new byte[1024]);
			acm = ActiveCodeUtils.receiveMessage(socket1, new byte[1024]);
			System.out.println("It still proceeds here");
			if (acm == null){
				break;
			}
			
		}
		
		System.out.println("Jump out of the while loop");
		
	}
}
