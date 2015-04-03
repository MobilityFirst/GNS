package edu.umass.cs.gns.activecode.protocol;

import java.io.Serializable;

import edu.umass.cs.gns.util.ResultValue;
import edu.umass.cs.gns.util.ValuesMap;

public class ActiveCodeParams implements Serializable {
	public String guid;
	public String field;
	public String action;
	public String code;
	public String valuesMapString;
	public int hopLimit;
	
	public ActiveCodeParams(String guid, String field, String action, String code, String valuesMap, int hopLimit) {
		this.guid = guid;
		this.field = field;
		this.action = action;
		this.code = code;
		this.valuesMapString = valuesMap;
		this.hopLimit = hopLimit;
	}

	public ActiveCodeParams() {
		this.guid = null;
		this.field = null;
		this.action = null;
		this.code = null;
		this.valuesMapString = null;
		this.hopLimit = 0;
	}
}
