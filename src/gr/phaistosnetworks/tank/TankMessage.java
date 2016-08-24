package gr.phaistosnetworks.tank;

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
   * @param seqId sequence id
   * @param ts timestamp
   * @param key message key
   * @param message the data. It is arbitrary byte array, not string.
   */
  public TankMessage(long seqId, long ts, byte[] key, byte[] message) {
    this.seqId = seqId;
    this.timestamp = ts;
    this.message = message;
    this.key = key;

    if (key.length > 0) {
      haveKey = true;
    } else {
      haveKey = false;
    }
  }

  /**
   * Constructor without sequence id, i.e. for publish.
   *
   * @param key message key
   * @param message the data. It is arbitrary byte array, not string.
   */
  public TankMessage(byte[] key, byte[] message) {
    this.key = key;
    if (key.length > 0) {
      haveKey = true;
    } else {
      haveKey = false;
    }
    this.message = message;
  }

  /**
   * Constructor without sequence id or key, i.e. for publish.
   *
   * @param message the data. It is arbitrary byte array, not string.
   */
  public TankMessage(byte[] message) {
    haveKey = false;
    this.message = message;
  }

  /**
   * serialize tank message into byte array.
   *
   * @param useLastTs set current timestamp or skip and use last set timestamp
   * @return serialized byte array
   */
  public byte[] serialize(boolean useLastTs) throws IOException, TankException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    byte flags = 0;
    if (useLastTs) {
      flags |= TankClient.USE_LAST_SPECIFIED_TS;
    }
    if (haveKey) { 
      flags |= TankClient.HAVE_KEY;
    }
    baos.write(ByteManipulator.serialize(flags, TankClient.U8));

    if (!useLastTs) {
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
   * gets the message sequence id.
   *
   * @return the sequence id
   */
  public long getSeqId() {
    return seqId;
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
   * gets the message.
   *
   * @return the message
   */
  public byte[] getMessage() {
    return message;
  }

  /**
   * does this message have a key?.
   *
   * @return drumroll
   */
  public boolean haveKey() {
    return haveKey;
  }

  /**
   * Get the key.
   *
   * @return the key
   */
  public byte[] getKey() {
    return key;
  }

  private boolean haveKey;
  private long seqId;
  private long timestamp;
  private byte[] key;
  private byte[] message;
}
