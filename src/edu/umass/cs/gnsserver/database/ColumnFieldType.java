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
 *
 */
package edu.umass.cs.gnsserver.database;

/**
 * Types that a column field can take on.
 * 
 * SEEMS LIKE THIS COULD PROBABLY ALSO BE DONE 
 * BETTER USING THE JSON LIB.
 *
 */
public enum ColumnFieldType {

  /**
   * Column type that is a String.
   */
  STRING,

  /**
   * Column type that is a list of Strings.
   */
  LIST_STRING,

  /**
   * Column type that is a map of user values.
   */
  VALUES_MAP,

  /**
   * Column type which is a JSON Object.
   */
  USER_JSON // never stored in a system field
  ;

}
