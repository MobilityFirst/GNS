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

/**
 * This is the data structure for active code response
 *
 * @author mbadov
 */
@SuppressWarnings("serial")
public class ActiveCodeQueryResponse implements Serializable {

  private static final long serialVersionUID = 2326392043474125897L;
  
  private boolean success;
  private String valuesMapString;

  public ActiveCodeQueryResponse(boolean success, String valuesMapString) {
    this.success = success;
    this.valuesMapString = valuesMapString;
  }

  public ActiveCodeQueryResponse() {
    this.success = false;
    this.valuesMapString = null;
  }

  public boolean isSuccess() {
    return success;
  }

  public void setSuccess(boolean success) {
    this.success = success;
  }

  public String getValuesMapString() {
    return valuesMapString;
  }

  public void setValuesMapString(String valuesMapString) {
    this.valuesMapString = valuesMapString;
  }

}
