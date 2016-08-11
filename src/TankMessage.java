import java.io.ByteArrayOutputStream;
import java.io.IOException;

class TankMessage {

	public TankMessage(long sid, long ts, byte[] m) {
		seqID = sid;
		timestamp = ts;
		message = m;
	}

	public TankMessage(long ts, String m) {
		timestamp = ts;
		message = m.getBytes();
	}

	public byte[] serialize(byte flags, String key) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ByteManipulator gandalf = new ByteManipulator();
		try {
			baos.write(gandalf.serialize(flags, 8));
			if ((flags & TankClient.UseLastSpecifiedTS) == 0)
				baos.write(gandalf.serialize(System.currentTimeMillis(), 64));

			if ((flags & TankClient.HaveKey) == 1)
				baos.write(gandalf.getStr8(key));

			baos.write(gandalf.getVarInt(message.length));
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
