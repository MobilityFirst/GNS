package edu.umass.cs.gnsserver.activecode.prototype.interfaces;

import java.io.UnsupportedEncodingException;


public interface Message {
	

	public byte[] toBytes() throws UnsupportedEncodingException;
}
