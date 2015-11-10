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
package edu.umass.cs.gnsclient.client.http;

/**
 * This class defines UniversalGnsClient which extends UniversalHttpClient to
 * provide backwards naming compatibility.
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
@Deprecated
public class UniversalGnsClient extends UniversalHttpClient
{

  /**
   * Creates a new <code>UniversalGnsClient</code> object
   * 
   * @param host
   * @param port
   */
  public UniversalGnsClient(String host, int port)
  {
    super(host, port);
  }

  /**
   * Returns the gns value.
   * 
   * @return Returns the gns.
   */
  public static UniversalGnsClient getGns()
  {
    return (UniversalGnsClient) gns;
  }

}
