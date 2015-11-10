package edu.umass.cs.gnsserver.activecode.protocol;

import java.io.Serializable;


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

  public String getAction() {
    return action;
  }

  public void setAction(String action) {
    this.action = action;
  }

  public String getCode() {
    return code;
  }

  public void setCode(String code) {
    this.code = code;
  }

  public String getValuesMapString() {
    return valuesMapString;
  }

  public void setValuesMapString(String valuesMapString) {
    this.valuesMapString = valuesMapString;
  }

  public int getHopLimit() {
    return hopLimit;
  }

  public void setHopLimit(int hopLimit) {
    this.hopLimit = hopLimit;
  }
        
        
}
