package edu.umass.cs.gnsserver.activecode.prototype;

/**
 * @author gaozy
 *
 */
public interface ActiveChannel {
	
	/**
	 * @param buffer
	 * @return number of bytes read into buffer
	 */
	
	public int read(byte[] buffer);
	/**
	 * @param buffer
	 * @param offset
	 * @param length
	 * @return true if write succeeds, false otherwise
	 */
	public boolean write(byte[] buffer, int offset, int length);
	
	/**
	 * close the channel
	 */
	public void shutdown();
}
