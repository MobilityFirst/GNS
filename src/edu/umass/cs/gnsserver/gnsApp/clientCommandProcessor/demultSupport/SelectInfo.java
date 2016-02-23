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
 *  Initial developer(s): Abhigyan Sharma, Westy
 *
 */
package edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.demultSupport;

/**
 * This class represents a data structure to store information
 * about queries (SELECT like lookup) transmitted by the local name
 * server.
 */
@Deprecated
public class SelectInfo {

  private final int id;

  /**
   * Constructs a SelectInfo instance with the following parameters.
   *
   * @param id Query id
   */
  public SelectInfo(int id) {
    this.id = id;
  }

  /**
   * Returns the id.
   * 
   * @return the id
   */
  public int getId() {
    return id;
  }

}
