package edu.umass.cs.gnsserver.activecode.protocol;

import java.io.Serializable;


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

  public String getGuid() {
    return guid;
  }

  public void setGuid(String guid) {
    this.guid = guid;
  }

  public String getField() {
    return field;
  }

  public void setField(String field) {
    this.field = field;
  }

  public String getValuesMapString() {
    return valuesMapString;
  }

  public void setValuesMapString(String valuesMapString) {
    this.valuesMapString = valuesMapString;
  }

  public String getAction() {
    return action;
  }

  public void setAction(String action) {
    this.action = action;
  }
        
        
}
