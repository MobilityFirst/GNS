/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *  Initial developer(s): Misha Badov, Westy
 *
 */
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
