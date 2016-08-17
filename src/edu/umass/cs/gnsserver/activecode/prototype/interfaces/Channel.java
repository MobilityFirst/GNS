package edu.umass.cs.gnsserver.activecode.prototype.interfaces;

import java.io.IOException;

/**
 * This interface is used for Active GNS communication.
 * The implementation of this interface could be named pipe,
 * UDP, or any way for interprocess communication. It allows
 * substitution with UDP for named pipe if GNS is running 
 * on Windows or other non-Unix systems.
 * 
 * @author gaozy
 *
 */
public interface Channel {
	
	/** 
	 * Sends an ActiveMessage through channel.
	 * 
	 * @param msg
	 * @throws IOException if an I/O error occurs
	 */
	public void sendMessage(Message msg) throws IOException;
	
	/**
	 * Receive an ActiveMessage from channel. This method
	 * blocks until a message is available.
	 * 
	 * @return a message read from the channel
	 * @throws IOException if an I/O error occurs
	 */
	public Message receiveMessage() throws IOException;
	
	/**
	 * shutdown the channel
	 */
	public void shutdown();
}
