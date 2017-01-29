package edu.umass.cs.gnsserver.activecode.prototype.interfaces;


@Deprecated
public interface ActiveChannel {
	

	public int read(byte[] buffer);
	

	public boolean write(byte[] buffer, int offset, int length);
	

	public void shutdown();
}
