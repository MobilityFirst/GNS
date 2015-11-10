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
package edu.umass.cs.gnsclient.client.http.android;

import java.io.IOException;

import edu.umass.cs.gnsclient.client.http.AbstractHttpClient;

/**
 * This class defines a AndroidGnrsClient
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
@Deprecated
public class AndroidHttpClient extends AbstractHttpClient
{

  /**
   * Creates a new <code>AndroidGnrsClient</code> object
   * 
   * @param host
   * @param port
   */
  public AndroidHttpClient(String host, int port)
  {
    super(host, port);
  }

  /**
   * @see edu.umass.cs.gnrs.client.AbstractGnrsClient#checkConnectivity()
   */
  @Override
  public void checkConnectivity() throws IOException
  {
    String urlString = "http://" + host + ":" + port + "/";
    final httpGet httpGet = new httpGet();
    httpGet.execute(urlString);
    try
    {
      Object httpGetResponse = httpGet.get();
      if (httpGetResponse instanceof IOException)
        throw (IOException) httpGetResponse;
    }
    catch (Exception e)
    {
      throw new IOException(e);
    }
  }

  /**
   * @see edu.umass.cs.gnrs.client.AbstractGnrsClient#sendGetCommand(java.lang.String)
   */
  @Override
  protected String sendGetCommand(String queryString) throws IOException
  {
    String urlString = "http://" + host + ":" + port + "/GNS/" + queryString;
    final httpGet httpGet = new httpGet();
    httpGet.execute(urlString);
    try
    {
      Object httpGetResponse = httpGet.get();
      if (httpGetResponse instanceof IOException)
        throw (IOException) httpGetResponse;
      else
        return (String) httpGetResponse;
    }
    catch (Exception e)
    {
      throw new IOException(e);
    }
  }

  private class httpGet extends DownloadTask
  {
    /**
     * Creates a new <code>httpGet</code> object
     */
    public httpGet()
    {
      super();
    }

    // onPostExecute displays the results of the AsyncTask.
    @Override
    protected void onPostExecute(Object result)
    {
    }

  }

}
