
package edu.umass.cs.gnsserver.httpserver;


import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsParameters;
import com.sun.net.httpserver.HttpsServer;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;


public class GNSHttpsServer extends GNSHttpServer {
  private HttpsServer httpsServer = null;

  private final static Logger LOG = Logger.getLogger(GNSHttpsServer.class.getName());


  public GNSHttpsServer(int port, ClientRequestHandlerInterface requestHandler) {
    super(port, requestHandler);
  }


  @Override
  public final void stop() {
    if (httpsServer != null) {
      httpsServer.stop(0);
    }
  }


  @Override
  public boolean tryPort(int port) {
    try {
      InetSocketAddress addr = new InetSocketAddress(port);
      httpsServer = HttpsServer.create(addr, 0);

      SSLContext sslContext = SSLContext.getInstance("TLS");

      char[] keyStorePassword = System.getProperty("javax.net.ssl.keyStorePassword").toCharArray();
      FileInputStream inputStream = new FileInputStream(System.getProperty("javax.net.ssl.keyStore"));
      KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
      keyStore.load(inputStream, keyStorePassword);

      // setup the key manager factory
      KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance("SunX509");
      keyManagerFactory.init(keyStore, keyStorePassword);

      // setup the trust manager factory
      TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance("SunX509");
      trustManagerFactory.init(keyStore);

      // setup the HTTPS context and parameters
      sslContext.init(keyManagerFactory.getKeyManagers(), trustManagerFactory.getTrustManagers(), null);
      httpsServer.setHttpsConfigurator(new HttpsConfigurator(sslContext) {
        @Override
        public void configure(HttpsParameters parameters) {
          try {
            // initialise the SSL context
            SSLContext context = SSLContext.getDefault();
            SSLEngine engine = context.createSSLEngine();
            parameters.setNeedClientAuth(false);
            parameters.setCipherSuites(engine.getEnabledCipherSuites());
            parameters.setProtocols(engine.getEnabledProtocols());

            // get the default parameters
            SSLParameters defaultSSLParameters = context.getDefaultSSLParameters();
            parameters.setSSLParameters(defaultSSLParameters);

          } catch (NoSuchAlgorithmException e) {
            LOG.log(Level.SEVERE, "Failed to configure HTTPS service: " + e);
          }
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
              new Object[]{port, e});
      return false;
    } catch (IOException | NoSuchAlgorithmException | KeyStoreException 
            | CertificateException | UnrecoverableKeyException | KeyManagementException e) {
      LOG.log(Level.FINE,
              "HTTPS server failed to start on port {0} due to {1}",
              new Object[]{port, e});
      e.printStackTrace();
      return false;
    }
  }
}
