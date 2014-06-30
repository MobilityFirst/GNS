package edu.umass.cs.gns.nio.nioutils;
/**
@author V. Arun
 */
import java.nio.channels.SocketChannel;

import edu.umass.cs.gns.nio.InterfaceDataProcessingWorker;

/* DefaultDataProcessingWorker simply prints the received data.
 */
public class DataProcessingWorkerDefault implements InterfaceDataProcessingWorker {	
	public void processData(SocketChannel socket, byte[] data, int count) {
		System.out.println("Received: " + new String(data, 0, count));
	}	
}
