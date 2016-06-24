package edu.umass.cs.gnsserver.activecode.prototype;

/**
 * @author gaozy
 *
 */
public interface ActiveChannel {
	
	/**
	 * @param buffer
	 * @return
	 */
	
	public int read(byte[] buffer);
	/**
	 * @param buffer
	 * @param offset
	 * @param length
	 * @return
	 */
	public boolean write(byte[] buffer, int offset, int length);
	
	public void shutdown();
}
