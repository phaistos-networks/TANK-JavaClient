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
   * @param k message key
   * @param m the data. It is arbitrary byte array, not string.
   */
  public TankMessage(long sid, long ts, byte[] k, byte[] m) {
    seqID = sid;
    timestamp = ts;
    message = m;
    key = k;

    if (k.length > 0) {
      haveKey = true;
    } else {
      haveKey = false;
    }
  }

  /**
   * Constructor without sequence id, i.e. for publish.
   *
   * @param k message key
   * @param m the data. It is arbitrary byte array, not string.
   */
  public TankMessage(byte[] k, byte[] m) {
    key = k;
    if (k.length > 0) {
      haveKey = true;
    } else {
      haveKey = false;
    }
    message = m;
  }

  /**
   * Constructor without sequence id or key, i.e. for publish.
   *
   * @param m the data. It is arbitrary byte array, not string.
   */
  public TankMessage(byte[] m) {
    haveKey = false;
    message = m;
  }

  /**
   * serialize tank message into byte array.
   *
   * @param useLastTS set current timestamp or skip and use last set TS
   * @return serialized byte array
   */
  public byte[] serialize(boolean useLastTS) throws IOException, TankException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    byte flags = 0;
    if (useLastTS) {
      flags |= TankClient.USE_LAST_SPECIFIED_TS;
    }
    if (haveKey) { 
      flags |= TankClient.HAVE_KEY;
    }
    baos.write(ByteManipulator.serialize(flags, TankClient.U8));

    if (!useLastTS) {
      baos.write(ByteManipulator.serialize(System.currentTimeMillis(), TankClient.U64));
    }

    if (haveKey) {
      baos.write(ByteManipulator.getStr8(key));
    }

    baos.write(ByteManipulator.getVarInt(message.length));
    baos.write(message);
    return baos.toByteArray();
  }

  /**
   * gets the message sequence id
   *
   * @return the sequence id
   */
  public long getSeqID() {
    return seqID;
  }

  /**
   * gets the message's timestamp.
   *
   * @return the timestamp
   */
  public long getTimestamp() {
    return timestamp;
  }

  /**
   * gets the message
   *
   * @return the message
   */
  public byte[] getMessage() {
    return message;
  }

  /**
   * does this message have a key?
   *
   * @return drumroll
   */
  public boolean haveKey() {
    return haveKey;
  }

  /**
   * Get the key
   *
   * @return the key
   */
  public byte[] getKey() {
    return key;
  }

  private boolean haveKey;
  private long seqID;
  private long timestamp;
  private byte[] key;
  private byte[] message;
}
