package edu.umass.cs.nio;

/**
 * @author V. Arun
 * 
 *         This interface should be implemented by all packet types using NIO.
 */
public interface IntegerPacketType {
	/**
	 * @return The integer value corresponding to this packet type.
	 */
	public int getInt();
}
