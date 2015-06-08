package edu.umass.cs.nio.nioutils;

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
	public void processData(SocketChannel socket, byte[] data, int count) {
		System.out.println("Received: " + new String(data, 0, count));
	}
}
