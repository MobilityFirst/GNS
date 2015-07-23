package edu.umass.cs.gns.newApp;

import edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.GnsProtocolDefs;
import edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.MetaDataTypeName;
import edu.umass.cs.gns.database.ColumnField;
import edu.umass.cs.gns.database.ColumnFieldType;
import edu.umass.cs.gns.exceptions.FailedDBOperationException;
import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.newApp.recordmap.NameRecord;
import edu.umass.cs.gns.newApp.clientSupport.NSAuthentication;
import edu.umass.cs.gns.newApp.clientSupport.NSGroupAccess;
import edu.umass.cs.gns.newApp.packet.DNSPacket;
import edu.umass.cs.gns.newApp.packet.DNSRecordType;
import edu.umass.cs.gns.newApp.recordmap.BasicRecordMap;
import edu.umass.cs.gns.util.NSResponseCode;
import edu.umass.cs.gns.util.ValuesMap;
import edu.umass.cs.utils.DelayProfiler;
import org.json.JSONException;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.List;

/**
 * This class executes lookupJSONArray requests sent by an LNS to an active replica. If name servers are replicated,
 * then methods in this class will be executed after the coordination among active replicas at name servers
 * is complete.
 *
 * Created by abhigyan on 2/27/14.
 */
public class AppLookup {

  private static final ArrayList<ColumnField> dnsSystemFields = new ArrayList<ColumnField>();

  static {
    dnsSystemFields.add(NameRecord.ACTIVE_VERSION);
    dnsSystemFields.add(NameRecord.TIME_TO_LIVE);
  }

  /**
   *
   * @param dnsPacket
   * @param gnsApp
   * @param doNotReplyToClient
   * @throws java.io.IOException
   * @throws org.json.JSONException
   * @throws java.security.InvalidKeyException
   * @throws java.security.spec.InvalidKeySpecException
   * @throws java.security.NoSuchAlgorithmException
   * @throws java.security.SignatureException
   * @throws edu.umass.cs.gns.exceptions.FailedDBOperationException
   */
  public static void executeLookupLocal(DNSPacket dnsPacket, GnsApplicationInterface gnsApp,
          boolean doNotReplyToClient)
          throws IOException, JSONException, InvalidKeyException,
          InvalidKeySpecException, NoSuchAlgorithmException, SignatureException, FailedDBOperationException {
    Long receiptTime = System.currentTimeMillis(); // instrumentation
    if (AppReconfigurableNodeOptions.debuggingEnabled) {
      GNS.getLogger().info("Node " + gnsApp.getNodeID().toString() + "; DNS Query Packet: " + dnsPacket.toString());
    }
    // FIX THIS!
    // META COMMENT ABOUT THE COMMENT BELOW: 
    // Originally the responder field was used to communicate back to the client about which node responded to a query.
    // Now it appears someone is using it for another purpose, undocumented. This seems like a bad idea.
    // Get your own field! - Westy
    //
    // if all replicas are coordinating on a read request, then check if this node should reply to client.
    // whether coordination is done or not, only the replica receiving client's request replies to the client.
    if (dnsPacket.getResponder() != null && // null means that we are not coordinating with other replicas upon reads.
            // so this replica should reply to client
            !dnsPacket.getResponder().equals(gnsApp.getNodeID())) {
      return;
    }

      // NOW WE DO THE ACTUAL LOOKUP
    // But first we do signature and ACL checks
    String guid = dnsPacket.getGuid();
    String field = dnsPacket.getKey();
    List<String> fields = dnsPacket.getKeys();
    String reader = dnsPacket.getAccessor();
    String signature = dnsPacket.getSignature();
    String message = dnsPacket.getMessage();
    // Check the signature and access
    NSResponseCode errorCode = NSResponseCode.NO_ERROR;

    if (reader != null) { // reader will be null for internal system reads
      if (field != null) {// single field check
        errorCode = NSAuthentication.signatureAndACLCheck(guid, field, reader, signature, message,
                MetaDataTypeName.READ_WHITELIST, gnsApp, dnsPacket.getCCPAddress());
      } else { //multi field check - return an error if any field doesn't pass
        for (String key : fields) {
          NSResponseCode code;
          if ((code = NSAuthentication.signatureAndACLCheck(guid, key, reader, signature,
                  message, MetaDataTypeName.READ_WHITELIST, gnsApp, dnsPacket.getCCPAddress())).isAnError()) {
            errorCode = code;
          }
        }
      }
    }

    DelayProfiler.updateDelay("totalLookupAuth", receiptTime);
    // return an error packet if one of the checks doesn't pass
    if (errorCode.isAnError()) {
      dnsPacket.getHeader().setQRCode(DNSRecordType.RESPONSE);
      dnsPacket.getHeader().setResponseCode(errorCode);
      dnsPacket.setResponder(gnsApp.getNodeID());
      if (AppReconfigurableNodeOptions.debuggingEnabled) {
        GNS.getLogger().fine("Sending to " + dnsPacket.getCCPAddress() + " this error packet "
                + dnsPacket.toJSONObjectForErrorResponse());
      }
      if (!doNotReplyToClient) {
        gnsApp.getClientCommandProcessor().injectPacketIntoCCPQueue(dnsPacket.toJSONObjectForErrorResponse());
        //gnsApp.getMessenger().sendToAddress(dnsPacket.getCCPAddress(), dnsPacket.toJSONObjectForErrorResponse());
      }
    } else {
        // All signature and ACL checks passed see if we can find the field to return;

      //Otherwise we do a standard lookup
      NameRecord nameRecord = lookupNameRecordLocally(dnsPacket, guid, field, fields, gnsApp.getDB());
      try {
          // But before we continue handle the group guid indirection case, but only
        // if the name record doesn't contain the field we are looking for
        // and only for single field lookups.
        if (AppReconfigurableNodeOptions.allowGroupGuidIndirection && field != null && !GnsProtocolDefs.ALLFIELDS.equals(field)
                && nameRecord != null && !nameRecord.containsKey(field) && !doNotReplyToClient) {
          if (handlePossibleGroupGuidIndirectionLookup(dnsPacket, guid, field, nameRecord, gnsApp)) {
            // We got the values and sent them out above so we're done here.
            return;
          }
        }
      } catch (FieldNotFoundException e) {
        if (AppReconfigurableNodeOptions.debuggingEnabled) {
          GNS.getLogger().info("Field not found: " + field + " fields: " + fields);
        }
      }

      if (AppReconfigurableNodeOptions.debuggingEnabled) {
        GNS.getLogger().info("Name record read is: " + nameRecord);
      }
        // Now we either have a name record with stuff it in or a null one
      // Time to send something back to the client
      dnsPacket = checkAndMakeResponsePacket(dnsPacket, nameRecord, gnsApp);
      if (!doNotReplyToClient) {
        gnsApp.getClientCommandProcessor().injectPacketIntoCCPQueue(dnsPacket.toJSONObject());
        //gnsApp.getMessenger().sendToAddress(dnsPacket.getCCPAddress(), dnsPacket.toJSONObject());
      }
      DelayProfiler.updateDelay("totalLookup", receiptTime);
    }
  }

  /**
   * Special case group guid field indirection lookup:
   * If the guid is a group guid and it is a single field lookup we try to get the values from the
   * members of the group.
   *
   * If it is a group guid we use the group to get a bunch of values for a field.
   * We only do this for a single field lookup!
   *
   * @param dnsPacket
   * @param guid
   * @param field
   * @param nameRecord
   * @param gnsApp
   * @return
   * @throws FailedDBOperationException
   * @throws IOException
   * @throws JSONException
   */
  private static boolean handlePossibleGroupGuidIndirectionLookup(DNSPacket dnsPacket, String guid, String field, NameRecord nameRecord,
          GnsApplicationInterface gnsApp) throws FailedDBOperationException, IOException, JSONException {
    if (NSGroupAccess.isGroupGuid(guid, gnsApp)) {
      ValuesMap valuesMap = NSGroupAccess.lookupFieldInGroupGuid(guid, field, gnsApp, dnsPacket.getCCPAddress());
      // Set up the response packet
      dnsPacket.getHeader().setQRCode(DNSRecordType.RESPONSE);
      dnsPacket.setResponder(gnsApp.getNodeID());
      dnsPacket.getHeader().setResponseCode(NSResponseCode.NO_ERROR);
      dnsPacket.setRecordValue(valuesMap);
      // .. and send it
      gnsApp.getClientCommandProcessor().injectPacketIntoCCPQueue(dnsPacket.toJSONObject());
      //gnsApp.getMessenger().sendToAddress(dnsPacket.getCCPAddress(), dnsPacket.toJSONObject());
      return true;
    }
    return false;
  }

  /**
   * Does the actual lookup of the field or fields in the database.
   *
   * @param dnsPacket
   * @param guid
   * @param field
   * @param fields
   * @param database
   * @return
   * @throws FailedDBOperationException
   */
  private static NameRecord lookupNameRecordLocally(DNSPacket dnsPacket, String guid, String field, List<String> fields,
          BasicRecordMap database) throws FailedDBOperationException {
    NameRecord nameRecord = null;
    // Try to look up the value in the database
    try {
      // Check for the case where we're returning all the fields the entire record.
      if (GnsProtocolDefs.ALLFIELDS.equals(field)) {
        if (AppReconfigurableNodeOptions.debuggingEnabled) {
          GNS.getLogger().fine("Field=" + field + " Format=" + dnsPacket.getReturnFormat());
        }
        // need everything so just grab all the fields
        nameRecord = NameRecord.getNameRecord(database, guid);
        // Otherwise if field is specified we're just looking up that single field.
      } else if (field != null) {
        if (AppReconfigurableNodeOptions.debuggingEnabled) {
          GNS.getLogger().fine("Field=" + field + " Format=" + dnsPacket.getReturnFormat());
        }
        // otherwise grab a few system fields we need plus the field the user wanted
        nameRecord = NameRecord.getNameRecordMultiField(database, guid, dnsSystemFields, dnsPacket.getReturnFormat(), field);
        // Last case: If "field" is null we're going to look in "fields" for a list of fields to lookup
      } else { // multi-field lookup
        if (AppReconfigurableNodeOptions.debuggingEnabled) {
          GNS.getLogger().fine("Fields=" + fields + " Format=" + dnsPacket.getReturnFormat());
        }
        String[] fieldArray = new String[fields.size()];
        fieldArray = fields.toArray(fieldArray);
        // Grab a few system fields and the fields the user wanted
        nameRecord = NameRecord.getNameRecordMultiField(database, guid, dnsSystemFields, dnsPacket.getReturnFormat(), fieldArray);
      }
    } catch (RecordNotFoundException e) {
      GNS.getLogger().fine("Record not found for name: " + guid + " Key = " + field);
    }
    return nameRecord;
  }

  /**
   * Handles the normal case of returning a valid record plus
   * a few different cases of the record not being found.
   *
   * @param dnsPacket
   * @param nameRecord
   * @return
   */
  private static DNSPacket checkAndMakeResponsePacket(DNSPacket dnsPacket, NameRecord nameRecord, GnsApplicationInterface gnsApp) {
    // change it to a response packet
    dnsPacket.getHeader().setQRCode(DNSRecordType.RESPONSE);
    dnsPacket.setResponder(gnsApp.getNodeID());
    String guid = dnsPacket.getGuid();
    String key = dnsPacket.getKey();
    try {
      // Normative case... NameRecord was found and this server is one
      // of the active servers of the record
      if (nameRecord != null && nameRecord.getActiveVersion() != NameRecord.NULL_VALUE_ACTIVE_VERSION) {
        // does this check make sense? how can we find a nameRecord if the guid is null?
        if (guid != null) {
          //Generate the response packet
          // assume no error... change it below if there is an error
          dnsPacket.getHeader().setResponseCode(NSResponseCode.NO_ERROR);
          dnsPacket.setTTL(nameRecord.getTimeToLive());
          // instrumentation
          //dnsPacket.setLookupTime(nameRecord.getLookupTime());
          // Either returing one value or a bunch
          if (key != null && nameRecord.containsKey(key)) {
            // if it's a USER JSON (new return format) access just return the entire map
            if (ColumnFieldType.USER_JSON.equals(dnsPacket.getReturnFormat())) {
              dnsPacket.setRecordValue(nameRecord.getValuesMap());
            } else {
              // we return the single value of the key (old array-based return format)
              dnsPacket.setSingleReturnValue(nameRecord.getKeyAsArray(key));
              if (AppReconfigurableNodeOptions.debuggingEnabled) {
                GNS.getLogger().info("NS sending DNS lookup response: Name = " + guid + " Key = " + key + " Data = " + dnsPacket.getRecordValue());
              }
            }
            // or we're supposed to return all the keys so return the entire record
          } else if (dnsPacket.getKeys() != null || GnsProtocolDefs.ALLFIELDS.equals(key)) {
            dnsPacket.setRecordValue(nameRecord.getValuesMap());
            if (AppReconfigurableNodeOptions.debuggingEnabled) {
              GNS.getLogger().info("NS sending multiple value DNS lookup response: Name = " + guid);
            }
            // or we don't actually have the field
          } else { // send error msg.
            if (AppReconfigurableNodeOptions.debuggingEnabled) {
              GNS.getLogger().info("Record doesn't contain field: " + key + " guid = " + guid + " record = " + nameRecord.toString());
            }
            dnsPacket.getHeader().setResponseCode(NSResponseCode.FIELD_NOT_FOUND_ERROR);
          }
          // For some reason the Guid of the packet is null
        } else { // send error msg.
          if (AppReconfigurableNodeOptions.debuggingEnabled) {
            GNS.getLogger().info("GUID of query is NULL!");
          }
          dnsPacket.getHeader().setResponseCode(NSResponseCode.BAD_GUID_ERROR);
        }
        // we're not the correct active name server so tell the client that
      } else { // send invalid error msg.
        dnsPacket.getHeader().setResponseCode(NSResponseCode.ERROR_INVALID_ACTIVE_NAMESERVER);
        if (nameRecord == null) {
          if (AppReconfigurableNodeOptions.debuggingEnabled) {
            GNS.getLogger().info("Invalid actives. Name = " + guid);
          }
        }
      }
      // this handles the myriad of FieldNotFoundException's, which are mostly system
      // fields not found, not the user field in question
    } catch (FieldNotFoundException e) {
      if (AppReconfigurableNodeOptions.debuggingEnabled) {
        GNS.getLogger().severe("Field not found exception: " + e.getMessage());
      }
      dnsPacket.getHeader().setResponseCode(NSResponseCode.ERROR);
    }
    return dnsPacket;

  }

}
