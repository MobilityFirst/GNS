package edu.umass.cs.gnsserver.interfaces;

public interface InternalRequestHeader {


	public static final int DEFAULT_TTL = 5;


	public long getOriginatingRequestID();


	public String getOriginatingGUID();


	public int getTTL();


	public boolean hasBeenCoordinatedOnce();


	default String getQueryingGUID() {
		return getOriginatingGUID();
	}
	

	default void markInternal(boolean internal) {
		// do nothing
	}
	

	default boolean verifyInternal() {
		return false;
	}	
}
