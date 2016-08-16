package gr.phaistosnetworks.TANK;

import java.io.ByteArrayOutputStream;
import java.io.IOException;


/**
 * A tank message is an object with sequence id, ts and data.
 *
 * @author Robert Krambovitis @rkrambovitis
 */
public class TankMessage {

    /**
     * Constructor with sequence id, i.e. for response.
     *
     * @param sid sequence id
     * @param ts timestamp
     * @param m the data. It is arbitrary byte array, not string.
     */
    public TankMessage(long sid, long ts, byte[] m) {
        seqID = sid;
        timestamp = ts;
        message = m;
    }

    /**
     * Constructor without sequence id, i.e. for publish.
     *
     * @param ts timestamp
     * @param m the data. It is arbitrary byte array, not string.
     */
    public TankMessage(long ts, byte[] m) {
        timestamp = ts;
        message = m;
    }

    /**
     * serialize tank message into byte array.
     *
     * @param flags the message flags. See TANK tank_encoding.md
     * @param key the message key.
     * @return serialized byte array.
     */
    public byte[] serialize(byte flags, String key) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            baos.write(ByteManipulator.serialize(flags, TankClient.U8));
            if ((flags & TankClient.USE_LAST_SPECIFIED_TS) == 0)
                baos.write(ByteManipulator.serialize(System.currentTimeMillis(), TankClient.U64));

            if ((flags & TankClient.HAVE_KEY) == 1)
                baos.write(ByteManipulator.getStr8(key));

            baos.write(ByteManipulator.getVarInt(message.length));
            baos.write(message);
        } catch (IOException e) {
            System.exit(1);
        }
        return baos.toByteArray();
    }

    /**
     * gets the message sequence id
     *
     * @return the sequence id
     */
    public long getSeqID() { return seqID; }

    /**
     * gets the message's timestamp.
     *
     * @return the timestamp
     */
    public long getTimestamp() { return timestamp; }

    /**
     * gets the message
     *
     * @return the message
     */
    public byte[] getMessage() { return message; }

    private long seqID;
    private long timestamp;
    private byte[] message;
}
