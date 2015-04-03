package edu.umass.cs.gns.activecode.protocol;

import java.io.Serializable;

import edu.umass.cs.gns.util.ResultValue;

public class ActiveCodeQueryRequest implements Serializable {
	public String guid;
	public String field;
	public String valuesMapString;
	public String action;
	
	public ActiveCodeQueryRequest(String guid, String field, String valuesMapString, String action) {
		this.guid = guid;
		this.field = field;
		this.valuesMapString = valuesMapString;
		this.action = action;
	}
	
	public ActiveCodeQueryRequest() {
		this.guid = null;
		this.field = null;
		this.valuesMapString = null;
		this.action = null;
	}
}
