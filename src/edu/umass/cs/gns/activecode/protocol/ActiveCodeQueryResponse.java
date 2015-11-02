package edu.umass.cs.gns.activecode.protocol;

import java.io.Serializable;

public class ActiveCodeQueryResponse implements Serializable {
	public boolean success;
	public String valuesMapString;
	
	public ActiveCodeQueryResponse(boolean success, String valuesMapString) {
		this.success = success;
		this.valuesMapString = valuesMapString;
	}
	
	public ActiveCodeQueryResponse() {
		this.success = false;
		this.valuesMapString = null;
	}
	
}
