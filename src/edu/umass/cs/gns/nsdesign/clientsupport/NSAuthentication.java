/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */

package edu.umass.cs.gns.nsdesign.clientsupport;

import edu.umass.cs.gns.clientsupport.GuidInfo;
import edu.umass.cs.gns.clientsupport.MetaDataTypeName;
import edu.umass.cs.gns.exceptions.FailedDBOperationException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.Config;
import edu.umass.cs.gns.nsdesign.gnsReconfigurable.GnsReconfigurable;
import edu.umass.cs.gns.util.NSResponseCode;
import java.net.InetSocketAddress;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;

/**
 *
 * @author westy
 */
public class NSAuthentication {

  public static NSResponseCode signatureAndACLCheck(String guid, String field, String reader, String signature, 
          String message, MetaDataTypeName access, GnsReconfigurable gnsApp, InetSocketAddress lnsAddress) 
          throws InvalidKeyException, InvalidKeySpecException, SignatureException, NoSuchAlgorithmException, FailedDBOperationException {
    GuidInfo guidInfo;
    GuidInfo readerGuidInfo;
    if ((guidInfo = NSAccountAccess.lookupGuidInfo(guid, gnsApp, lnsAddress)) == null) {
      if (Config.debuggingEnabled) {
        GNS.getLogger().info("Name " + guid + " key = " + field + ": BAD_GUID_ERROR");
      }
      return NSResponseCode.BAD_GUID_ERROR;
    }
    if (reader.equals(guid)) {
      readerGuidInfo = guidInfo;
    } else if ((readerGuidInfo = NSAccountAccess.lookupGuidInfo(reader, true, gnsApp, lnsAddress)) == null) {
      if (Config.debuggingEnabled) {
        GNS.getLogger().info("Name " + guid + " key = " + field + ": BAD_ACCESOR_ERROR");
      }
      return NSResponseCode.BAD_ACCESSOR_ERROR;
    }
    if (signature == null) {
      if (!NSAccessSupport.fieldAccessibleByEveryone(access, guidInfo.getGuid(), field, gnsApp)) {
        if (Config.debuggingEnabled) {
          GNS.getLogger().info("Name " + guid + " key = " + field + ": ACCESS_ERROR");
        }
        return NSResponseCode.ACCESS_ERROR;
      }
    } else if (signature != null) {
      if (!NSAccessSupport.verifySignature(readerGuidInfo, signature, message)) {
        if (Config.debuggingEnabled) {
          GNS.getLogger().info("Name " + guid + " key = " + field + ": SIGNATURE_ERROR");
        }
        return NSResponseCode.SIGNATURE_ERROR;
      } else if (!NSAccessSupport.verifyAccess(access, guidInfo, field, readerGuidInfo, gnsApp, lnsAddress)) {
        if (Config.debuggingEnabled) {
          GNS.getLogger().info("Name " + guid + " key = " + field + ": ACCESS_ERROR");
        }
        return NSResponseCode.ACCESS_ERROR;
      }
    }
    return NSResponseCode.NO_ERROR;
  }
  
}
