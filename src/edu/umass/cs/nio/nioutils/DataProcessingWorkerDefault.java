package edu.umass.cs.nio.nioutils;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import edu.umass.cs.nio.InterfaceDataProcessingWorker;

/**
 * @author V. Arun
 * 
 *         DefaultDataProcessingWorker simply prints the received data to
 *         standard output.
 */
public class DataProcessingWorkerDefault implements
		InterfaceDataProcessingWorker {
	public void processData(SocketChannel socket, ByteBuffer incoming) {
		byte[] rcvd = new byte[incoming.remaining()];
		incoming.get(rcvd);
		System.out.println("Received: " + new String(rcvd));
	}
}
