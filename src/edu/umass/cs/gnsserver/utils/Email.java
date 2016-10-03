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
package edu.umass.cs.gnsserver.utils;

import edu.umass.cs.gnscommon.utils.Format;
import com.sun.mail.smtp.SMTPTransport;
import com.sun.mail.util.MailSSLSocketFactory;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.gnsserver.main.GNSConfig.GNSC;
import edu.umass.cs.utils.Config;
import java.security.GeneralSecurityException;
import java.util.Date;
import javax.mail.Message;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.mail.MessagingException;

/**
 * Contains a few different methods to send email to the recipient.
 *
 * @author westy
 */
public class Email {

  private static final Logger LOG = Logger.getLogger(Email.class.getName());

  /**
   * @return Logger used by most of the client support package.
   */
  public static final Logger getLogger() {
    return LOG;
  }

  /**
   * Attempts using a few different methods to send email to the recipient.
   *
   * @param subject
   * @param recipient
   * @param text
   * @return true if successful
   */
  public static boolean email(String subject, String recipient, String text) {
    if (!GNSC.isDontTryLocalEmail() && emailLocal(subject, recipient, text, true)) {
      return true;
    } else if (simpleMail(subject, recipient, text, true)) {
      return true;
    } else if (emailSSL(subject, recipient, text, true)) {
      return true;
    } else if (emailTLS(subject, recipient, text, true)) {
      return true;
      //now run it again with error messages turned on
    } else if (!GNSC.isDontTryLocalEmail() && emailLocal(subject, recipient, text, false)) {
      return true;
    } else if (simpleMail(subject, recipient, text, false)) {
      return true;
    } else if (emailSSL(subject, recipient, text, false)) {
      return true;
    } else if (emailTLS(subject, recipient, text, false)) {
      return true;
    } else {
      getLogger().log(Level.WARNING, "Unable to send email to {0}", recipient);
      return false;
    }
  }
  /* To send to multiple users
   void addRecipients(Message.RecipientType type, 
   Address[] addresses)
   throws MessagingException
   */
 /* authentication 

   props.setProperty("mail.user", "myuser");
   props.setProperty("mail.password", "mypwd");

   */

  private static final String SMTP_HOST = "smtp.gmail.com";

  public static boolean simpleMail(String subject, String recipient, String text) {
    return simpleMail(subject, recipient, text, true);
  }

  public static boolean simpleMail(String subject, String recipient, String text, boolean suppressWarning) {
    try {
      MailSSLSocketFactory sf = new MailSSLSocketFactory();
      sf.setTrustAllHosts(true);
      Properties props = new Properties();

      props.setProperty("mail.smtp.ssl.enable", "true");
      //props.put("mail.smtp.host", smtpHost);
      props.put("mail.smtp.auth", "true");
      props.put("mail.smtp.ssl.socketFactory", sf);
      Session session = Session.getInstance(props);

      Message message = new MimeMessage(session);
      message.setFrom(new InternetAddress(Config.getGlobalString(GNSConfig.GNSC.SUPPORT_EMAIL)));
      message.setRecipients(Message.RecipientType.TO,
              InternetAddress.parse(recipient));
      message.setSubject(subject);
      message.setText(text);
      SMTPTransport t = (SMTPTransport) session.getTransport("smtp");
      try {
        t.connect(SMTP_HOST, Config.getGlobalString(GNSConfig.GNSC.ADMIN_EMAIL),
                Config.getGlobalString(GNSConfig.GNSC.ADMIN_PASSWORD));
        t.sendMessage(message, message.getAllRecipients());
        getLogger().log(Level.FINE, "Email response: {0}", t.getLastServerResponse());
      } finally {
        t.close();
      }
      getLogger().log(Level.FINE, "Successfully sent email to {0} with message: {1}", new Object[]{recipient, text});
      return true;
    } catch (GeneralSecurityException | MessagingException e) {
      if (!suppressWarning) {
        getLogger().log(Level.WARNING, "Unable to send email: {0}", e);
      }
      return false;
    }
  }

  /**
   * Attempts to use SSL to send a message to the recipient.
   *
   * @param subject
   * @param recipient
   * @param text
   * @return true if the message was sent
   */
  public static boolean emailSSL(String subject, String recipient, String text) {
    return emailSSL(subject, recipient, text, false);
  }

  /**
   * Attempts to use SSL to send a message to the recipient.
   * If suppressWarning is true no warning message will be logged if this fails.
   *
   * @param subject
   * @param recipient
   * @param text
   * @param suppressWarning
   * @return true if the message was sent
   */
  public static boolean emailSSL(String subject, String recipient, String text, boolean suppressWarning) {
    try {
      Properties props = new Properties();
      props.put("mail.smtp.host", SMTP_HOST);
      props.put("mail.smtp.socketFactory.port", "465");
      props.put("mail.smtp.socketFactory.class",
              "javax.net.ssl.SSLSocketFactory");
      props.put("mail.smtp.auth", "true");
      props.put("mail.smtp.port", "465");

      Session session = Session.getInstance(props,
              new javax.mail.Authenticator() {
        @Override
        protected PasswordAuthentication getPasswordAuthentication() {
          return new PasswordAuthentication(Config.getGlobalString(GNSConfig.GNSC.ADMIN_EMAIL), 
                  Config.getGlobalString(GNSConfig.GNSC.ADMIN_PASSWORD));
        }
      });

      Message message = new MimeMessage(session);
      message.setFrom(new InternetAddress(Config.getGlobalString(GNSConfig.GNSC.SUPPORT_EMAIL)));
      message.setRecipients(Message.RecipientType.TO,
              InternetAddress.parse(recipient));
      message.setSubject(subject);
      message.setText(text);

      Transport.send(message);
      getLogger().log(Level.FINE,
              "Successfully sent email to {0} with message: {1}", new Object[]{recipient, text});
      return true;

    } catch (Exception e) {
      if (!suppressWarning) {
        getLogger().log(Level.WARNING, "Unable to send email: {0}", e);
      }
      return false;
    }
  }

  /**
   * Attempts to use TLS to send a message to the recipient.
   *
   * @param subject
   * @param recipient
   * @param text
   * @return true if the message was sent
   */
  public static boolean emailTLS(String subject, String recipient, String text) {
    return emailTLS(subject, recipient, text, false);
  }

  /**
   * Attempts to use TLS to send a message to the recipient.
   * If suppressWarning is true no warning message will be logged if this fails.
   *
   * @param subject
   * @param recipient
   * @param text
   * @param suppressWarning
   * @return true if the message was sent
   */
  // TLS doesn't work with Dreamhost
  public static boolean emailTLS(String subject, String recipient, String text, boolean suppressWarning) {
    final String username = Config.getGlobalString(GNSConfig.GNSC.ADMIN_EMAIL);
    final String contactString = Config.getGlobalString(GNSConfig.GNSC.ADMIN_PASSWORD);

    try {
      Properties props = new Properties();
      props.put("mail.smtp.auth", "true");
      props.put("mail.smtp.starttls.enable", "true");
      props.put("mail.smtp.host", "smtp.gmail.com");
      props.put("mail.smtp.port", "587");

      Session session = Session.getInstance(props,
              new javax.mail.Authenticator() {
        @Override
        protected PasswordAuthentication getPasswordAuthentication() {
          return new PasswordAuthentication(username, contactString);
        }
      });

      Message message = new MimeMessage(session);
      message.setFrom(new InternetAddress(Config.getGlobalString(GNSConfig.GNSC.SUPPORT_EMAIL)));
      message.setRecipients(Message.RecipientType.TO,
              InternetAddress.parse(recipient));
      message.setSubject(subject);
      message.setText(text);

      Transport.send(message);
      getLogger().log(Level.FINE,
              "Successfully sent email to {0} with message: {1}", new Object[]{recipient, text});
      return true;

    } catch (Exception e) {
      if (!suppressWarning) {
        getLogger().log(Level.WARNING, "Unable to send email: {0}", e);
      }
      return false;
    }
  }

  /**
   * Attempts to use the local emailer to send a message to the recipient.
   *
   * @param subject
   * @param recipient
   * @param text
   * @return true if the message was sent
   */
  public static boolean emailLocal(String subject, String recipient, String text) {
    return emailLocal(subject, recipient, text, false);
  }

  /**
   * Attempts to use the local emailer to send a message to the recipient.
   * If suppressWarning is true no warning message will be logged if this fails.
   *
   * @param subject
   * @param recipient
   * @param text
   * @param suppressWarning
   * @return true if the message was sent
   */
  public static boolean emailLocal(String subject, String recipient, String text, boolean suppressWarning) {
    // Get system properties
    Properties properties = System.getProperties();

    properties.setProperty("mail.smtp.host", "localhost");
    //properties.setProperty("mail.user", "westy");
    //properties.setProperty("mail.password", "");

    // Get the default Session object.
    Session session = Session.getDefaultInstance(properties);

    try {
      // Create a default MimeMessage object.
      MimeMessage message = new MimeMessage(session);

      // Set From: header field of the header.
      message.setFrom(new InternetAddress(Config.getGlobalString(GNSConfig.GNSC.SUPPORT_EMAIL)));

      // Set To: header field of the header.
      message.addRecipient(Message.RecipientType.TO,
              new InternetAddress(recipient));

      // Set Subject: header field
      message.setSubject(subject);

      // Now set the actual message
      message.setText(text);

      // Send message
      Transport.send(message);
      getLogger().log(Level.FINE, "Successfully sent email to {0} with message: {1}", new Object[]{recipient, text});
      return true;
    } catch (Exception e) {
      if (!suppressWarning) {
        getLogger().log(Level.WARNING, "Unable to send email: {0}", e);
      }
      return false;
    }
  }

  public static void main(String[] args) {
    email("hello", "westy@cs.umass.edu", 
            "This is another email on " + Format.formatPrettyDateUTC(new Date()) + "."
            + "\n The support email is " + Config.getGlobalString(GNSConfig.GNSC.SUPPORT_EMAIL) + "."
            + "Thanks, \nHave a nice day."
    );
  }
}
