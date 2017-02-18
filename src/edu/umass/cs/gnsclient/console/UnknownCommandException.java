/* Copyright (c) 2017 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * Initial developer(s): Westy */
package edu.umass.cs.gnsclient.console;

/**
 * This class defines a ServerException
 */
public class UnknownCommandException extends ConsoleException {

  private static final long serialVersionUID = 6627620787610127842L;

  /**
   * Creates a new <code>UnknownCommandException</code> object
   */
  public UnknownCommandException() {
    super();
  }

  /**
   * Creates a new <code>UnknownCommandException</code> object
   *
   * @param message
   * @param cause
   */
  public UnknownCommandException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Creates a new <code>UnknownCommandException</code> object
   *
   * @param message
   */
  public UnknownCommandException(String message) {
    super(message);
  }

  /**
   * Creates a new <code>UnknownCommandException</code> object
   *
   * @param throwable
   */
  public UnknownCommandException(Throwable throwable) {
    super(throwable);
  }

}
