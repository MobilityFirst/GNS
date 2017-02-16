/*
 *
 *  Copyright (c) 2017 University of Massachusetts
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
 */
package edu.umass.cs.gnsserver.httpserver;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.SocketException;
import java.net.URL;
import java.security.KeyStore;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;

/**
 * A test class to verify proper functioning of client authorization in the HTTP server.
 */
public class AuthTestClient {

  private static String NO_KEYSTORE = "";
  private static String AUTH_KEYSTORE = System.getProperty("javax.net.ssl.keyStore");
  private static String TRUSTSTORE = System.getProperty("javax.net.ssl.trustStore");
  private static String CLIENT_PWD = System.getProperty("javax.net.ssl.keyStorePassword");

  public static void main(String[] args) throws Exception {
    AuthTestClient cl = new AuthTestClient();
    System.out.println("No keystore:");
    cl.testIt(NO_KEYSTORE);
    System.out.println("Auth keystore:");
    cl.testIt(AUTH_KEYSTORE);
  }

  private void testIt(String jksFile) {
    try {
      String https_url = "https://localhost:24803/";
      URL url;
      url = new URL(https_url);
      HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();
      conn.setSSLSocketFactory(getSSLFactory(jksFile));

      conn.setRequestMethod("GET");
      conn.setDoOutput(true);
      conn.setUseCaches(false);

      try (BufferedReader bir = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
        String line;
        while ((line = bir.readLine()) != null) {
          System.out.println(line);
        }
      }
      conn.disconnect();
    } catch (SSLHandshakeException | SocketException e) {
      System.out.println(e.getMessage());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static SSLSocketFactory getSSLFactory(String jksFile) throws Exception {
    // Create key store
    KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
    KeyManager[] kmfs = null;
    if (jksFile.length() > 0) {
      keyStore.load(new FileInputStream(jksFile), CLIENT_PWD.toCharArray());
      KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      kmf.init(keyStore, CLIENT_PWD.toCharArray());
      kmfs = kmf.getKeyManagers();
    }

    // create trust store (validates the self-signed server!)
    KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
    trustStore.load(new FileInputStream(TRUSTSTORE), CLIENT_PWD.toCharArray());
    TrustManagerFactory trustFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    trustFactory.init(trustStore);

    SSLContext sslContext = SSLContext.getInstance("TLS");
    sslContext.init(kmfs, trustFactory.getTrustManagers(), null);
    return sslContext.getSocketFactory();
  }

  // Fixes the "java.security.cert.CertificateException: No name matching localhost found" issue
  // For localhost testing only
  static {
    javax.net.ssl.HttpsURLConnection.setDefaultHostnameVerifier(
            new javax.net.ssl.HostnameVerifier() {
      @Override
      public boolean verify(String hostname,
              javax.net.ssl.SSLSession sslSession) {
        return hostname.equals("localhost");
      }
    });
  }
}
