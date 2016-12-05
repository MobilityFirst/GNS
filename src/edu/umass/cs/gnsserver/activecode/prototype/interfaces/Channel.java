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
	 * Sends an ActiveMessage through channel. If this
	 * method returns without an exception, it means
	 * the message has been sent with reliably.
	 * <p> This method throws an IOException if the
	 * channel is corrupted, and can not be used
	 * anymore.
	 * <p> This is an unblocking method, so it's
	 * implementer's responsibility to guarantee the
	 * message can be successfully sent through this
	 * channel. For example, there is no guarantee
	 * for named pipe to keep the messages intact
	 * if multiple threads writing into a same pipe
	 * at the same time, the messages can become intermingled.
	 * So this method must be used exclusively by
	 * different threads for named pipe.
	 * 
	 * 
	 * @param msg the message to send
	 * @throws IOException if an I/O error occurs
	 */
	public void sendMessage(Message msg) throws IOException;
	
	/**
	 * Receive an ActiveMessage from channel. This method
	 * blocks until a message is available.
	 * <p> An IOException is thrown if the channel is corrupted
	 * or shutdown. 
	 * 
	 * @return a message read from the channel
	 * @throws IOException if an I/O error occurs
	 */
	public Message receiveMessage() throws IOException;
	
	/**
	 * shutdown the channel
	 */
	public void close();
}
