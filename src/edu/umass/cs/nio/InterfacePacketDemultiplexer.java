package edu.umass.cs.nio;

/**
 * @author V. Arun
 * @param <MessageType> 
 */
public interface InterfacePacketDemultiplexer<MessageType> {
	/**
	 * @param message
	 * @return The return value should return true if the handler handled the
	 *         message and doesn't want any other BasicPacketDemultiplexer to
	 *         handle the message.
	 */

	public boolean handleMessage(MessageType message);
}
