package edu.umass.cs.gns.packet;

import edu.umass.cs.gns.packet.Packet.PacketType;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * ***********************************************************
 * This class implements the packet transmitted to send an admin request.
 *
 * @author Westy
 ***********************************************************
 */
public class AdminRequestPacket extends BasicPacket {

    public enum AdminOperation {

        DELETEALLRECORDS, CLEARCACHE, 
        //DELETEALLGUIDRECORDS, 
        RESETDB
    };
    public final static String ID = "id";
    private final static String OPERATION = "operation";
    private final static String ARGUMENT = "arg";
    //
    private int id;
    private AdminOperation operation;
    private String argument;

    /**
     * ***********************************************************
     * Constructs new ActiveNameServerPacket with the give parameter.
     *
     * @param primaryNameserver Primary name server
     * @param localNameserver Local name server sending the request
     * @param name Host/domain/device name
     * @param activeNameserver Set containing ids of active name servers
   ***********************************************************
     */
    public AdminRequestPacket(AdminOperation operation) {
        this(operation, null);
    }

    public AdminRequestPacket(AdminOperation operation, String argument) {
        this.type = PacketType.ADMIN_REQUEST;
        this.id = 0;
        this.operation = operation;
        this.argument = argument;
    }

    /**
     * ***********************************************************
     * Constructs new ActiveNameServerPacket from a JSONObject
     *
     * @param json JSONObject representing this packet
     * @throws JSONException
   ***********************************************************
     */
    public AdminRequestPacket(JSONObject json) throws JSONException {
        if (Packet.getPacketType(json) != PacketType.ADMIN_REQUEST) {
            Exception e = new Exception("AdminRequestPacket: wrong packet type " + Packet.getPacketType(json));
            e.printStackTrace();
            return;
        }

        this.type = Packet.getPacketType(json);
        this.id = json.getInt(ID);
        this.operation = AdminOperation.valueOf(json.getString(OPERATION));
        this.argument = json.optString(ARGUMENT, null);
    }

    /**
     * ***********************************************************
     * Converts a ActiveNSUpdatePacket to a JSONObject.
     *
     * @return JSONObject representing this packet.
     * @throws JSONException
   ***********************************************************
     */
    @Override
    public JSONObject toJSONObject() throws JSONException {
        JSONObject json = new JSONObject();
        Packet.putPacketType(json, getType());
        json.put(ID, id);
        json.put(OPERATION, getOperation().name());
        if (this.argument != null) {
            json.put(ARGUMENT, argument);
        }
        return json;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public AdminOperation getOperation() {
        return operation;
    }

    public String getArgument() {
        return argument;
    }
    
    
}
