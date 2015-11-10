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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;

import android.os.AsyncTask;
import android.util.Log;

/**
 * This class defines a DownloadTask
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class DownloadTask extends AsyncTask<String, String, Object>
{
  private static final String LOG_TAG = "GNS";

  @Override
  protected Object doInBackground(String... urls)
  {
    String stringUrl = urls[0];
    // params comes from the execute() call: params[0] is the url.
    try
    {
      return downloadUrl(stringUrl);
    }
    catch (IOException e)
    {
      return "Unable to retrieve web page. URL may be invalid.";
    }
  }

  // Given a URL, establishes an HttpUrlConnection and retrieves
  // the web page content as a InputStream, which it returns as
  // a string.
  private Object downloadUrl(String myUrl) throws IOException
  {
    InputStream is = null;
    try
    {
      URL url = new URL(myUrl);
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setReadTimeout(10000 /* milliseconds */);
      conn.setConnectTimeout(15000 /* milliseconds */);
      conn.setRequestMethod("GET");
      conn.setDoInput(true);
      Log.v(LOG_TAG, "HTTP GET: " + myUrl);
      // Starts the query
      conn.connect();
      int response = conn.getResponseCode();
      Log.v(LOG_TAG, "HTTP response code: " + response);
      is = conn.getInputStream();

      // Convert the InputStream into a string
      String contentAsString = readIt(is);
      Log.v(LOG_TAG, "HTTP content: " + contentAsString);
      return contentAsString;

      // Makes sure that the InputStream is closed after the app is
      // finished using it.
    }
    catch (Exception e)
    {
      Log.e(LOG_TAG, "HTTP error on: " + myUrl, e);
      return e;
    }
    finally
    {
      if (is != null)
      {
        is.close();
      }
    }
  }

  // Reads an InputStream and converts it to a String.
  private String readIt(InputStream stream) throws IOException,
      UnsupportedEncodingException
  {
    BufferedReader rd = new BufferedReader(new InputStreamReader(stream));

    String response = null;
    int cnt = 3;
    do
    {
      try
      {
        response = rd.readLine(); // we only expect one line to be sent
        break;
      }
      catch (java.net.SocketTimeoutException e)
      {
        Log.i(LOG_TAG, "Get Response timed out. Trying " + cnt + " more times.");
      }
    }
    while (cnt-- > 0);
    Log.d(LOG_TAG, "Received: " + response);
    return response;
  }

}
