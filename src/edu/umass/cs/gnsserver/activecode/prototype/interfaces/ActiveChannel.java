package edu.umass.cs.gnsserver.activecode.prototype.interfaces;

/**
 * @author gaozy
 *
 */
public interface ActiveChannel {
	
	/**
	 * Read the content into the buffer, and return the number of bytes being read.
	 * @param buffer
	 * @return number of bytes read into buffer
	 */	
	public int read(byte[] buffer);
	
	/**
	 * Write the content in buffer into the channel.
	 * @param buffer
	 * @param offset
	 * @param length
	 * @return true if write succeeds, false otherwise
	 */
	public boolean write(byte[] buffer, int offset, int length);
	
	/**
	 * Close the channel to clear the channel state.
	 */
	public void shutdown();
}
