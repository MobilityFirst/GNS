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
 *  Initial developer(s): Westy
 *
 */
package edu.umass.cs.gnsserver.exceptions;

/**
 * Exception means that the desired database operation could not be completed.
 * Probably because the database server is crashed.
 *
 * This exception does not tell much about the cause of the error. Therefore, if this exception
 * is seen, one can only attempt to retry the operation hoping that the database 
 * unavailability is transient. If indeed the database is permanently crashed, 
 * an external mechanism would be needed to restart the database.
 * 
 */
public class FailedDBOperationException extends GnsException {
  String collection;
  String name;

  /**
   * Create a FailedDBOperationException instance.
   * 
   * @param collection
   * @param name
   */
  public FailedDBOperationException(String collection, String name) {
    this.collection = collection;
    this.name = name;
  }

  @Override
  public String getMessage() {
    return "FailedDBOperationException: " + " Collection = " + collection + " Insert = " + name;
  }
}
