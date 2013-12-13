/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.util;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartLocalNameServer;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.Properties;

public class Email {

  public static final String ACCOUNT_CONTACT_EMAIL = "admin@gnrs.name";
  private static final String password = "deadDOG8";
  private static final String smtpHost = "mail.gnrs.name";

  /**
   * Example Email application
   *
   * @author Westy
   */
  /**
   * Usage: java Email [username] [text]
   *
   * @param args message
   */
  public static void main(String[] args) {
    if (args.length < 2) {
      System.out.println("Usage: Email [to] [message]");
      System.exit(-1);
    } else {
      boolean result = emailSSL("Testing Subject SSL", args[0], args[1]);
      System.exit(result ? 0 : -1);
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
  public static boolean emailSSL(String subject, String recipient, String text) {
    if (StartLocalNameServer.noEmail) return true;
    Properties props = new Properties();
    props.put("mail.smtp.host", smtpHost);
    props.put("mail.smtp.socketFactory.port", "465");
    props.put("mail.smtp.socketFactory.class",
            "javax.net.ssl.SSLSocketFactory");
    props.put("mail.smtp.auth", "true");
    props.put("mail.smtp.port", "465");

    Session session = Session.getDefaultInstance(props,
            new javax.mail.Authenticator() {
              @Override
              protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(ACCOUNT_CONTACT_EMAIL, password);
              }
            });
    try {
      Message message = new MimeMessage(session);
      message.setFrom(new InternetAddress(ACCOUNT_CONTACT_EMAIL));
      message.setRecipients(Message.RecipientType.TO,
              InternetAddress.parse(recipient));
      message.setSubject(subject);
      message.setText(text);

      Transport.send(message);
      GNS.getLogger().fine("Successfully sent email to " + recipient + " with message: " + text);
      return true;

    } catch (MessagingException e) {
      GNS.getLogger().warning("Unable to send email: " + e);
      return false;
    }
  }

  // TLS doesn't work with Dreamhost
  public static boolean emailTLS(String subject, String recipient, String text) {
    final String username = "bear@westy.org";
    final String password = "deadDOG8";

    Properties props = new Properties();
    props.put("mail.smtp.auth", "true");
    props.put("mail.smtp.starttls.enable", "true");
    props.put("mail.smtp.host", "smtp.gmail.com");
    props.put("mail.smtp.port", "587");

    Session session = Session.getInstance(props,
            new javax.mail.Authenticator() {
              @Override
              protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(username, password);
              }
            });

    try {

      Message message = new MimeMessage(session);
      message.setFrom(new InternetAddress(ACCOUNT_CONTACT_EMAIL));
      message.setRecipients(Message.RecipientType.TO,
              InternetAddress.parse(recipient));
      message.setSubject(subject);
      message.setText(text);

      Transport.send(message);
      GNS.getLogger().fine("Successfully sent email to " + recipient + " with message: " + text);
      return true;

    } catch (MessagingException e) {
      GNS.getLogger().warning("Unable to send email: " + e);
      e.printStackTrace();
      return false;
    }
  }

  public static boolean emailLocal(String subject, String recipient, String text) {
    // Get system properties
    Properties properties = System.getProperties();

    properties.setProperty("mail.smtp.host", "localhost");
    properties.setProperty("mail.user", "westy");
    properties.setProperty("mail.password", "");

    // Get the default Session object.
    Session session = Session.getDefaultInstance(properties);

    try {
      // Create a default MimeMessage object.
      MimeMessage message = new MimeMessage(session);

      // Set From: header field of the header.
      message.setFrom(new InternetAddress(ACCOUNT_CONTACT_EMAIL));

      // Set To: header field of the header.
      message.addRecipient(Message.RecipientType.TO,
              new InternetAddress(recipient));

      // Set Subject: header field
      message.setSubject(subject);

      // Now set the actual message
      message.setText(text);

      // Send message
      Transport.send(message);
      GNS.getLogger().fine("Successfully sent email to " + recipient + " with message: " + text);
      return true;
    } catch (MessagingException mex) {
      GNS.getLogger().warning("Unable to send email: " + mex);
      mex.printStackTrace();
      return false;
    }

  }
}
