package edu.umass.cs.gnsserver.activecode;

import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;

/**
 * @author gaozy
 *
 */
public class ActiveCodeInternalRequestHeader implements InternalRequestHeader{
	
	public ActiveCodeInternalRequestHeader(){
		
	}
	
	@Override
	public long getOriginatingRequestID() {
		throw new RuntimeException();
	}

	@Override
	public String getOriginatingGUID() {
		throw new RuntimeException();
	}

	@Override
	public int getTTL() {
		throw new RuntimeException();
	}
	
}
