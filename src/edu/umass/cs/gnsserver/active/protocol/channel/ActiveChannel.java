package edu.umass.cs.gnsserver.active.protocol.channel;

import java.io.IOException;

/**
 * @author gaozy
 *
 */
public interface ActiveChannel {
	
	/**
	 * The method is used to write an object into the channel, and channel
	 * sends the data to the other end of the channel.
	 * @param msg 
	 * @return true if succeed, false otherwise
	 * @throws IOException 
	 */
	public boolean write(Object obj) throws IOException;
	
	/**
	 * The method is used to read the data from the channel, and put the
	 * data into the buffer.
	 * @param buf
	 * @return an object recovered from the buffer
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 */
	public Object read(byte[] buf) throws IOException, ClassNotFoundException;
}
