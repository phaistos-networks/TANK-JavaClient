package gr.phaistosnetworks.TANK;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class TankMessage {

    public TankMessage(long sid, long ts, byte[] m) {
        seqID = sid;
        timestamp = ts;
        message = m;
    }

    public TankMessage(long ts, byte[] m) {
        timestamp = ts;
        message = m;
    }

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

    public long getSeqID() { return seqID; }
    public long getTimestamp() { return timestamp; }
    public byte[] getMessage() { return message; }

    private long seqID;
    private long timestamp;
    private byte[] message;
}
