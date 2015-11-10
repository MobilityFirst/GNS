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
 *  Initial developer(s): Westy, Emmanuel Cecchet
 *
 */
package edu.umass.cs.gnsclient.exceptions;

/**
 * This class defines a GnsInvalidUserException
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class GnsInvalidUserException extends GnsException
{
  private static final long serialVersionUID = -9116661769609344698L;

  /**
   * Creates a new <code>GnsInvalidUserException</code> object
   */
  public GnsInvalidUserException()
  {
    // TODO Auto-generated constructor stub
  }

  /**
   * Creates a new <code>GnsInvalidUserException</code> object
   * 
   * @param message
   * @param cause
   */
  public GnsInvalidUserException(String message, Throwable cause)
  {
    super(message, cause);
    // TODO Auto-generated constructor stub
  }

  /**
   * Creates a new <code>GnsInvalidUserException</code> object
   * 
   * @param message
   */
  public GnsInvalidUserException(String message)
  {
    super(message);
    // TODO Auto-generated constructor stub
  }

  /**
   * Creates a new <code>GnsInvalidUserException</code> object
   * 
   * @param throwable
   */
  public GnsInvalidUserException(Throwable throwable)
  {
    super(throwable);
    // TODO Auto-generated constructor stub
  }

}
