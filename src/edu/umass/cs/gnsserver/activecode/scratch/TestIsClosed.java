package edu.umass.cs.gnsserver.activecode.scratch;

import java.net.DatagramSocket;
import java.net.SocketException;

public class TestIsClosed {
	public static void main(String[] args) throws SocketException{
		DatagramSocket socket = new DatagramSocket();
		long t = System.nanoTime();
		for (int i =0; i<1000; i++){
			socket.isClosed();
		}
		long eclapsed = System.nanoTime() - t;
		
		System.out.println("It takes "+eclapsed/1000+"ns");
	}
}
