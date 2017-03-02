/*
 * Copyright (C) 2016
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gnsclient.client.singletests.failingtests;

import edu.umass.cs.gnsclient.client.GNSClientConfig.GNSCC;
import edu.umass.cs.gnsclient.client.http.HttpClient;
import edu.umass.cs.gnsserver.httpserver.GNSHttpProxy;
import edu.umass.cs.utils.Config;
import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 *
 * @author Brendan
 * This is a direct copy of Westy's HttpClientTest.  I only changed the port number to be the
 * GNSHttpProxy default.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class HttpProxyTest extends HttpClientTest {
	private static GNSHttpProxy proxy;

  /**
   *
   */
  public HttpProxyTest() {
	  httpClient = new HttpClient("127.0.0.1", Config.getGlobalInt(GNSCC.HTTP_PROXY_PORT));
  }
  
  /**
   * Launch a new GNSHttpProxy for these tests.
   */
  @BeforeClass
  public static void setupBeforeClass(){

	  int port = Config.getGlobalInt(GNSCC.HTTP_PROXY_PORT);
	  String hostname = Config.getGlobalString(GNSCC.HTTP_PROXY_INCOMING_HOSTNAME);
	  proxy = new GNSHttpProxy();
	  proxy.runServer(port, hostname);
  }
  
  /**
   * Stop the GNSHttpProxy that was running for these tests.
   */
  @AfterClass
  public static void teardownAfterClass(){
	  proxy.stop();
  }
}
