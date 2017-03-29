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
package edu.umass.cs.gnscommon.exceptions.server;

/**
 * This class defines a GnsException
 */
public class ServerRuntimeException extends RuntimeException
{
  private static final long serialVersionUID = 6627620787610127842L;

  /**
   * Creates a new <code>ServerRuntimeException</code> object
   */
  public ServerRuntimeException()
  {
    super();
  }

  /**
   * Creates a new <code>ServerRuntimeException</code> object
   * 
   * @param message
   * @param cause
   */
  public ServerRuntimeException(String message, Throwable cause)
  {
    super(message, cause);
  }

  /**
   * Creates a new <code>ServerRuntimeException</code> object
   * 
   * @param message
   */
  public ServerRuntimeException(String message)
  {
    super(message);
  }

  /**
   * Creates a new <code>ServerRuntimeException</code> object
   * 
   * @param throwable
   */
  public ServerRuntimeException(Throwable throwable)
  {
    super(throwable);
  }

}
