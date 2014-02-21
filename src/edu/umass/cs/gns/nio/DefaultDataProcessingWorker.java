package edu.umass.cs.gns.nio;
/**
@author V. Arun
 */
import java.nio.channels.SocketChannel;

/* DefaultDataProcessingWorker simply prints the received data.
 */
public class DefaultDataProcessingWorker implements DataProcessingWorker {	
	public void processData(SocketChannel socket, byte[] data, int count) {
		System.out.println("Received: " + new String(data, 0, count));
	}	
}
