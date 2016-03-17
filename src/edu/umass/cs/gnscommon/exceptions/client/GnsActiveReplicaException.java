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
package edu.umass.cs.gnscommon.exceptions.client;

/**
 * This class defines a GnsActiveReplicaException
 * 
 * @version 1.0
 */
public class GnsActiveReplicaException extends GnsClientException
{
  private static final long serialVersionUID = 2676899572105162853L;

  /**
   * Creates a new <code>GnsInvalidFieldException</code> object
   */
  public GnsActiveReplicaException()
  {
    // TODO Auto-generated constructor stub
  }

  /**
   * Creates a new <code>GnsInvalidFieldException</code> object
   * 
   * @param detailMessage
   */
  public GnsActiveReplicaException(String detailMessage)
  {
    super(detailMessage);
    // TODO Auto-generated constructor stub
  }

  /**
   * Creates a new <code>GnsInvalidFieldException</code> object
   * 
   * @param throwable
   */
  public GnsActiveReplicaException(Throwable throwable)
  {
    super(throwable);
    // TODO Auto-generated constructor stub
  }

  /**
   * Creates a new <code>GnsInvalidFieldException</code> object
   * 
   * @param detailMessage
   * @param throwable
   */
  public GnsActiveReplicaException(String detailMessage, Throwable throwable)
  {
    super(detailMessage, throwable);
    // TODO Auto-generated constructor stub
  }

}
