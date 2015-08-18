package edu.umass.cs.nio.nioutils;

import java.net.InetSocketAddress;

/**
 * @author arun
 *
 */
public class NIOHeader {
	/**
	 * 
	 */
	public final InetSocketAddress sndr;
	/**
	 * 
	 */
	public final InetSocketAddress rcvr;

	/**
	 * @param sndr
	 * @param rcvr
	 */
	public NIOHeader(InetSocketAddress sndr, InetSocketAddress rcvr) {
		this.sndr = sndr;
		this.rcvr = rcvr;
	}
}
