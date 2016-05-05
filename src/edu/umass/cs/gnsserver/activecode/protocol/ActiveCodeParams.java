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
 * This class is used to pass in the parameters
 * into activecode task.
 *
 * @author Zhaoyu Gao
 */
@SuppressWarnings("serial")
public class ActiveCodeParams implements Serializable {
	/**
	 * Use guid getter to get its value
	 */
	public String guid;
	/**
	 * Use field getter to get its value
	 */
	public String field;
	/**
	 * Use action getter to get its value
	 */
	public String action;
	/**
	 * Use code getter to get its value
	 */
	public String code;
	/**
	 * Use valueMap getter to get its value
	 */
	public String valuesMapString;
	/**
	 * Use limit getter to get its value
	 */
	public int hopLimit;
	/**
	 * Initialize ActiveCodeParams
	 * @param guid
	 * @param field
	 * @param action
	 * @param code
	 * @param valuesMap
	 * @param hopLimit
	 */
	public ActiveCodeParams(String guid, String field, String action, String code, String valuesMap, int hopLimit) {
		this.guid = guid;
		this.field = field;
		this.action = action;
		this.code = code;
		this.valuesMapString = valuesMap;
		this.hopLimit = hopLimit;
	}
	
	/**
	 * Initialize ActiveCodeParams
	 */
	public ActiveCodeParams() {
		this.guid = null;
		this.field = null;
		this.action = null;
		this.code = null;
		this.valuesMapString = null;
		this.hopLimit = 0;
	}
	
	@Override
	public String toString(){
		return "{guid:"+(guid==null?"[NULL]":guid)+",field:"+(field==null?"[NULL]":field)+"}";
	}
	

  /**
   * guid getter
   *
   * @return guid
   */
  public String getGuid() {
    return guid;
  }

  /**
   * guid setter
   *
   * @param guid
   */
  public void setGuid(String guid) {
    this.guid = guid;
  }

  /**
   * field getter
   *
   * @return field
   */
  public String getField() {
    return field;
  }

  /**
   * field setter
   *
   * @param field
   */
  public void setField(String field) {
    this.field = field;
  }

  /**
   * action getter
   *
   * @return action
   */
  public String getAction() {
    return action;
  }

  /**
   * action setter
   *
   * @param action
   */
  public void setAction(String action) {
    this.action = action;
  }

  /**
   * code getter
   *
   * @return code
   */
  public String getCode() {
    return code;
  }

  /**
   * code setter
   *
   * @param code
   */
  public void setCode(String code) {
    this.code = code;
  }

  /**
   * result getter
   *
   * @return result
   */
  public String getValuesMapString() {
    return valuesMapString;
  }

  /**
   * result setter
   *
   * @param valuesMapString
   */
  public void setValuesMapString(String valuesMapString) {
    this.valuesMapString = valuesMapString;
  }

  /**
   * hot getter
   *
   * @return number of hops
   */
  public int getHopLimit() {
    return hopLimit;
  }

  /**
   * hop setter
   *
   * @param hopLimit
   */
  public void setHopLimit(int hopLimit) {
    this.hopLimit = hopLimit;
  }

}
