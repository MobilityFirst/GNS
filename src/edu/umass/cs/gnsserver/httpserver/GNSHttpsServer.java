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
package edu.umass.cs.gnsserver.httpserver;

/**
 *
 * @author westy
 */
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsParameters;
import com.sun.net.httpserver.HttpsServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import java.io.FileInputStream;
import java.net.BindException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManagerFactory;

/**
 * The secure HTTP server.
 */
public class GNSHttpsServer extends GNSHttpServer {

  private HttpsServer httpsServer = null;

  private final static Logger LOG = Logger.getLogger(GNSHttpsServer.class.getName());

  /**
   *
   * @param port
   * @param requestHandler
   */
  public GNSHttpsServer(int port, ClientRequestHandlerInterface requestHandler) {
    super(port, requestHandler);
  }

  /**
   * Try to start the http server at the port.
   *
   * @param port
   * @return true if it was started
   */
  @Override
  public boolean tryPort(int port) {
    try {
      InetSocketAddress addr = new InetSocketAddress(port);
      httpsServer = HttpsServer.create(addr, 0);
      SSLContext sslContext = createSSLContext();
      httpsServer.setHttpsConfigurator(new HttpsConfigurator(sslContext) {
        @Override
        public void configure(HttpsParameters parameters) {
          // initialise the SSL context
          SSLContext context = getSSLContext();
          SSLEngine engine = context.createSSLEngine();
          //parameters.setNeedClientAuth(false);
          parameters.setCipherSuites(engine.getEnabledCipherSuites());
          parameters.setProtocols(engine.getEnabledProtocols());

          // get the default parameters
          SSLParameters sslParameters = context.getDefaultSSLParameters();
          sslParameters.setNeedClientAuth(true);
          parameters.setNeedClientAuth(true);
          parameters.setSSLParameters(sslParameters);
        }
      });

      httpsServer.createContext("/", new EchoHttpHandler());
      httpsServer.createContext("/" + GNS_PATH, new DefaultHttpHandler());
      httpsServer.setExecutor(Executors.newCachedThreadPool());
      httpsServer.start();
      // Need to do this for the places where we expose the secure http service to the user
      requestHandler.setHttpsServerPort(port);

      LOG.log(Level.INFO,
              "HTTPS server is listening on port {0}", port);
      return true;
    } catch (BindException e) {
      LOG.log(Level.FINE,
              "HTTPS server failed to start on port {0} due to {1}",
              new Object[]{port, e.getMessage()});
      return false;
    } catch (IOException | NoSuchAlgorithmException | KeyStoreException | CertificateException | UnrecoverableKeyException | KeyManagementException e) {
      LOG.log(Level.FINE,
              "HTTPS server failed to start on port {0} due to {1}",
              new Object[]{port, e.getMessage()});
      e.printStackTrace();
      return false;
    }
  }

  private SSLContext createSSLContext()
          throws CertificateException, IOException, KeyManagementException, KeyStoreException,
          NoSuchAlgorithmException, UnrecoverableKeyException {

    char[] keyStorePassword = System.getProperty("javax.net.ssl.keyStorePassword").toCharArray();
    FileInputStream ksInputStream = new FileInputStream(System.getProperty("javax.net.ssl.keyStore"));
    KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
    keyStore.load(ksInputStream, keyStorePassword);
    // setup the key manager factory
    KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance("SunX509");
    keyManagerFactory.init(keyStore, keyStorePassword);

    char[] trustStorePassword = System.getProperty("javax.net.ssl.trustStorePassword").toCharArray();
    FileInputStream tsInputStream = new FileInputStream(System.getProperty("javax.net.ssl.trustStore"));
    KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
    trustStore.load(tsInputStream, trustStorePassword);
    // setup the trust manager factory
    TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance("SunX509");
    trustManagerFactory.init(trustStore);

    SSLContext sslContext = SSLContext.getInstance("TLS");
    // setup the HTTPS context and parameters
    sslContext.init(keyManagerFactory.getKeyManagers(), trustManagerFactory.getTrustManagers(), null);
    return sslContext;
  }

  /**
   * Stops the https server.
   */
  @Override
  public final void stop() {
    if (httpsServer != null) {
      httpsServer.stop(0);
    }
  }

}
