package edu.umass.cs.gns.nsdesign.activeReplica;

import edu.umass.cs.gns.clientsupport.Defs;
import edu.umass.cs.gns.clientsupport.GuidInfo;
import edu.umass.cs.gns.clientsupport.MetaDataTypeName;
import edu.umass.cs.gns.database.ColumnField;
import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartNameServer;
import edu.umass.cs.gns.nameserver.NameRecord;
//import edu.umass.cs.gns.nameserver.NameServer;
import edu.umass.cs.gns.nameserver.clientsupport.NSAccessSupport;
import edu.umass.cs.gns.nameserver.clientsupport.NSAccountAccess;
import edu.umass.cs.gns.nameserver.clientsupport.SiteToSiteQueryHandler;
import edu.umass.cs.gns.nsdesign.GNSMessagingTask;
import edu.umass.cs.gns.packet.DNSPacket;
import edu.umass.cs.gns.packet.DNSRecordType;
import edu.umass.cs.gns.packet.NSResponseCode;
import org.json.JSONException;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;

/**
 * This class executes lookup requests sent by an LNS to an active replica. If name servers are replicated,
 * then methods in this class will be executed after  the coordination among active replicas at name servers
 * is complete.
 *
 * Created by abhigyan on 2/27/14.
 */
public class Lookup {

  /**
   *
   * @param dnsPacket
   * @param activeReplica
   */
  public static GNSMessagingTask executeLookupLocal(DNSPacket dnsPacket, ActiveReplica activeReplica)
          throws IOException, JSONException, InvalidKeyException,
          InvalidKeySpecException, NoSuchAlgorithmException, SignatureException {

    GNSMessagingTask msgTask = null;

//    if (StartNameServer.debugMode) {
//      GNS.getLogger().fine("NS received DNS lookup request: " + incomingJSON);
//    }
//    DNSPacket dnsPacket = new DNSPacket(incomingJSON);

    // the only dns reponses we should see are coming in respone to SiteToSiteQueryHandler requests
    if (!dnsPacket.isQuery()) {
      // handle the special case of queries that were sent between name servers
      SiteToSiteQueryHandler.handleDNSResponsePacket(dnsPacket);
      return null;
      // todo handle this
    }

    // First we do signature and ACL checks
    String guid = dnsPacket.getGuid();
    String field = dnsPacket.getKey().getName();
    String reader = dnsPacket.getAccessor();
    String signature = dnsPacket.getSignature();
    String message = dnsPacket.getMessage();
    // Check the signature and access
    NSResponseCode errorCode = NSResponseCode.NO_ERROR;
    if (reader != null) { // reader will be null for internal system reads
      errorCode = NSResponseCode.NO_ERROR;
      // TODO Abhigyan: assuming signature and ACL check are always successful
      //signatureAndACLCheck(guid, field, reader, signature, message, MetaDataTypeName.READ_WHITELIST);
    }
    // return an error packet if one of the checks doesn't pass
    if (errorCode.isAnError()) {
      dnsPacket.getHeader().setQRCode(DNSRecordType.RESPONSE);
      dnsPacket.getHeader().setResponseCode(errorCode);
      // PUT THE ID OF THE NAME SERVER IN THE RESPONSE
      GNS.getLogger().warning("****PUT THE ID OF THE NAME SERVER IN THE RESPONSE");
      //dnsPacket.setResponder(NameServer.getNodeID());
      GNS.getLogger().fine("Sending to " + dnsPacket.getSenderId() + " this error packet " + dnsPacket.toJSONObjectForErrorResponse());
      msgTask = new GNSMessagingTask(dnsPacket.getSenderId(), dnsPacket.toJSONObjectForErrorResponse());
//      NameServer.returnToSender(dnsPacket.toJSONObjectForErrorResponse(), dnsPacket.getSenderId());
    } else {
      // All signature and ACL checks passed see if we can find the field to return;
      NameRecord nameRecord = null;
      // Try to look up the value in the database
      try {
        if (Defs.ALLFIELDS.equals(dnsPacket.getKey().getName())) {
          // need everything so just grab all the fields
          nameRecord = NameRecord.getNameRecord(activeReplica.getNameRecordDB(), guid);
        } else {
          // otherwise grab a few system fields we need plus the field the user wanted
          nameRecord = NameRecord.getNameRecordMultiField(activeReplica.getNameRecordDB(), guid, getDNSPacketFields(), field);
        }
      } catch (RecordNotFoundException e) {
        GNS.getLogger().fine("Record not found for name: " + guid + " Key = " + field);
      }
      // Now we either have a name record with stuff it in or a null one
      // Time to send something back to the client
      dnsPacket = checkAndMakeResponsePacket(dnsPacket, nameRecord, activeReplica);
      msgTask = new GNSMessagingTask(dnsPacket.getSenderId(), dnsPacket.toJSONObject());
    }
    return msgTask;
  }

  private static ArrayList<ColumnField> dnsField = new ArrayList<ColumnField>();

  private static ArrayList<ColumnField> getDNSPacketFields() {
    synchronized (dnsField) {
      if (dnsField.size() == 0) {
        dnsField.add(NameRecord.ACTIVE_NAMESERVERS);
        dnsField.add(NameRecord.TIME_TO_LIVE);
      }
      return dnsField;
    }
  }

  private static NSResponseCode signatureAndACLCheck(String guid, String field, String reader, String signature, String message, MetaDataTypeName access)
          throws InvalidKeyException, InvalidKeySpecException, SignatureException, NoSuchAlgorithmException {
    GuidInfo guidInfo, readerGuidInfo;
    if ((guidInfo = NSAccountAccess.lookupGuidInfo(guid)) == null) {
      GNS.getLogger().fine("Name " + guid + " key = " + field + ": BAD_GUID_ERROR");
      return NSResponseCode.BAD_GUID_ERROR;
    }
    if (reader.equals(guid)) {
      readerGuidInfo = guidInfo;
    } else if ((readerGuidInfo = NSAccountAccess.lookupGuidInfo(reader, true)) == null) {
      GNS.getLogger().fine("Name " + guid + " key = " + field + ": BAD_ACCESOR_ERROR");
      return NSResponseCode.BAD_ACCESOR_ERROR;
    }
    // unsigned case, must be world readable
    if (signature == null) {
      if (!NSAccessSupport.fieldAccessibleByEveryone(access, guidInfo.getGuid(), field)) {
        GNS.getLogger().fine("Name " + guid + " key = " + field + ": ACCESS_ERROR");
        return NSResponseCode.ACCESS_ERROR;
      }
      // signed case, check signature and access
    } else if (signature != null) {
      if (!NSAccessSupport.verifySignature(readerGuidInfo, signature, message)) {
        GNS.getLogger().fine("Name " + guid + " key = " + field + ": SIGNATURE_ERROR");
        return NSResponseCode.SIGNATURE_ERROR;
      } else if (!NSAccessSupport.verifyAccess(access, guidInfo, field, readerGuidInfo)) {
        GNS.getLogger().fine("Name " + guid + " key = " + field + ": ACCESS_ERROR");
        return NSResponseCode.ACCESS_ERROR;
      }
    }
    return NSResponseCode.NO_ERROR;
  }

  /**
   * Handles the normal case of returning a valid record plus
   * a few different cases of the record not being found.
   *
   * @param dnsPacket
   * @param nameRecord
   * @return
   */
  private static DNSPacket checkAndMakeResponsePacket(DNSPacket dnsPacket, NameRecord nameRecord, ActiveReplica activeReplica) {
    dnsPacket.getHeader().setQRCode(DNSRecordType.RESPONSE);
    // PUT THE ID OF THE NAME SERVER IN THE RESPONSE
    GNS.getLogger().warning("****PUT THE ID OF THE NAME SERVER IN THE RESPONSE");
    //dnsPacket.setResponder(NameServer.getNodeID());
    // change it to a response packet
    String guid = dnsPacket.getGuid();
    String key = dnsPacket.getKey().getName();
    try {
      // Normative case... NameRecord was found and this server is one
      // of the active servers of the record
      if (nameRecord != null && nameRecord.containsActiveNameServer(activeReplica.getNodeID())) {
        // how can we find a nameRecord if the guid is null?
        if (guid != null) {
          dnsPacket.setActiveNameServers(nameRecord.getActiveNameServers());
          //Generate the response packet
          // assume no error... change it below if there is an error
          dnsPacket.getHeader().setResponseCode(NSResponseCode.NO_ERROR);
          dnsPacket.setTTL(nameRecord.getTimeToLive());
          // Either returing one value or a bunch
          if (nameRecord.containsKey(key)) {
            dnsPacket.setSingleReturnValue(nameRecord.getKey(key));
            GNS.getLogger().fine("NS sending DNS lookup response: Name = " + guid);
          } else if (Defs.ALLFIELDS.equals(key)) {
            dnsPacket.setRecordValue(nameRecord.getValuesMap());
            GNS.getLogger().finer("NS sending multiple value DNS lookup response: Name = " + guid);
            // or we don't actually have the field
          } else { // send error msg.
            GNS.getLogger().fine("Record doesn't contain field: " + key + " name  = " + guid);
            dnsPacket.getHeader().setResponseCode(NSResponseCode.ERROR);
          }
          // For some reason the Guid of the packet is null
        } else { // send error msg.
          GNS.getLogger().fine("GUID of query is NULL!");
          dnsPacket.getHeader().setResponseCode(NSResponseCode.ERROR);
        }
        // we're not the correct active name server so tell the client that
      } else { // send invalid error msg.
        dnsPacket.getHeader().setResponseCode(NSResponseCode.ERROR_INVALID_ACTIVE_NAMESERVER);
        if (nameRecord == null) {
          GNS.getLogger().fine("Invalid actives. Name = " + guid);
        } else {
          GNS.getLogger().fine("Invalid actives. Name = " + guid + " Actives = " + nameRecord.getActiveNameServers());
        }
      }
    } catch (FieldNotFoundException e) {
      if (StartNameServer.debugMode) {
        GNS.getLogger().severe("Field not found exception: " + e.getMessage());
      }
      dnsPacket.getHeader().setResponseCode(NSResponseCode.ERROR);
    }
    return dnsPacket;

  }
}
