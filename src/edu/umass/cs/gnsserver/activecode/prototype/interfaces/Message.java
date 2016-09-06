package edu.umass.cs.gnsserver.activecode.prototype.interfaces;

import java.io.UnsupportedEncodingException;

/**
 * This interface is used for Channel to send and
 * receive message.
 * 
 * @author gaozy
 *
 */
public interface Message {
	
	/**
	 * This method serializes a message to a byte array.
	 * A constructor with the input of byte array is required.
	 * It throws an UnsupportedEncodingException if the string
	 * field in the message 
	 * @return a byte array 
	 * @throws UnsupportedEncodingException
	 */
	public byte[] toBytes() throws UnsupportedEncodingException;
}
