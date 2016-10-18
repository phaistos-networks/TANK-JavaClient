package gr.phaistosnetworks.tank;

import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import java.nio.ByteBuffer;

/**
 * Purpose: To perform magic tricks on bytes.
 */
public class ByteManipulator {
  /**
   * Empty constructor.
   */
  public ByteManipulator() {
    input = ByteBuffer.allocate(0);
    this.totalLength = 0;
  }

  /**
   * Constructor that sets thei ByteBuffer to be manipulated.
   *
   * @param input the ByteBuffer to be manipulated;
   */
  public ByteManipulator(ByteBuffer input) {
    this.input = input;
    this.totalLength = input.remaining();
  }

  /**
   * Appends a ButeBuffer to the remainder of the current one.
   *
   * @param toAppend the ByteBuffer to append.
   */
  public void append(ByteBuffer toAppend) {
    ByteBuffer souma = ByteBuffer.allocate(input.remaining() + toAppend.remaining());

    while (input.hasRemaining()) {
      souma.put(input.get());
    }

    while (toAppend.hasRemaining()) {
      souma.put(toAppend.get());
    }

    souma.flip();
    this.totalLength = souma.remaining();
    this.input = souma;
  }

  /**
   * Returns the next length bytes from the current ByteBuffer.
   * There is no health check. If you request more bytes than available, boom
   *
   * @param length the amount of bytes to return
   * @return a new ByteBuffer containing the requested length of bytes
   */
  public ByteBuffer getNextBytes(int length) {
    ByteBuffer nextBytes = ByteBuffer.allocate(length);
    for (int i = 0; i < length ; i++) {
      nextBytes.put(input.get());
    }
    return nextBytes;
  }

  /**
   * Compress data using the snappy library.
   *
   * @param data the data to compress
   * @return the compressed data.
   */
  public static byte[] snappyCompress(byte [] data) throws IOException {
    return Snappy.compress(data);
  }

  /**
   * Uncompress the next length bytes using the snappy library.
   *
   * @param length the amount of bytes to uncompress
   * @return the uncompressed data.
   */
  public byte[] snappyUncompress(long length) throws IOException {
    byte [] toUnCompress = new byte[(int)length];
    input.get(toUnCompress, 0, (int)length);
    return Snappy.uncompress(toUnCompress);
  }

  /**
   * Serializes a long into a byte array with length
   *
   * @param length the length of the returned byte array
   * @param data the long to process.
   * @return the serialized data.
   */
  public static byte[] serialize(long data, int length) {
    byte[] output = new byte[length];
    long shift = 0L;

    for (int i = 0; i < length; i++) {
      shift = i * Byte.SIZE;
      output[i] = (byte) (data >> shift);
    }
    return output;
  }

  /**
   * Deserializes the next length bytes and returns a long.
   *
   * @param length the amount of bytes to deserialize.
   * @return the value of those bytes.
   */
  public long deSerialize(int length) {
    long result = 0L;

    for (int i = 0, n = 0; i != length; ++i, n += Byte.SIZE) {
      long mask = input.get() & BYTE_MAX;
      result |= (mask << n);
    }

    return result;
  }

  /**
   * Sets the leftmost bit to 0
   * int is used due to need of unsigned bytes.
   *
   * @param toFlip the byte to flip
   * @return int containing the byte.
   */
  private int flipped(byte toFlip) {
    return asInt(toFlip & ~(1 << VARINT_BYTE_SHIFT_ONE));
  }

  /**
   * Flips the leftmost bit of the last byte of a long.
   *
   * @param toFlip the long that needs it's last byte flipped.
   * @return the flipped byte
   */
  private static byte asFlipped(long toFlip) {
    return (byte)(toFlip | (1 << VARINT_BYTE_SHIFT_ONE));
  }

  /**
   * Returns the integer value of a byte, as if it was unsigned.
   *
   * @param val the value to get integer value for.
   * @return positive integer value of that byte.
   */
  private int asInt(int val) {
    if (val < 0) {
      return (val + BYTE_MAX + 1);
    } else {
      return val;
    }
  }

  /**
   * Reads a varint from the ByteBuffer.
   *
   * @return the value of the varint
   */
  public long getVarInt() {
    long result = 0;
    int length = 0;
    this.flushOffset(true);

    if (asInt(input.get(0)) > VARINT_BYTE_MAX) {
      if (asInt(input.get(1)) > VARINT_BYTE_MAX) {
        if (asInt(input.get(2)) > VARINT_BYTE_MAX) {
          if (asInt(input.get(3)) > VARINT_BYTE_MAX) {
            length = 5;
            result |= flipped(input.get(0))
              | (flipped(input.get(1)) << VARINT_BYTE_SHIFT_ONE)
              | (flipped(input.get(2)) << VARINT_BYTE_SHIFT_TWO)
              | (flipped(input.get(3)) << VARINT_BYTE_SHIFT_THREE)
              | (asInt(input.get(4)) << VARINT_BYTE_SHIFT_FOUR);
          } else {
            length = 4;
            result |= flipped(input.get(0))
              | (flipped(input.get(1)) << VARINT_BYTE_SHIFT_ONE)
              | (flipped(input.get(2)) << VARINT_BYTE_SHIFT_TWO)
              | (asInt(input.get(3)) << VARINT_BYTE_SHIFT_THREE);
          }
        } else {
          length = 3;
          result |= flipped(input.get(0))
            | (flipped(input.get(1)) << VARINT_BYTE_SHIFT_ONE)
            | (asInt(input.get(2)) << VARINT_BYTE_SHIFT_TWO);
        }
      } else {
        length = 2;
        result |= flipped(input.get(0))
          | (asInt(input.get(1)) << VARINT_BYTE_SHIFT_ONE);
      }
    } else {
      result |= asInt(input.get(0));
      length = 1;
    }

    for (int i = 0; i < length; i++) {
      input.get();
    }
    return result;
  }


  /**
   * Transforms a long into a varint byte array.
   * The implementation is based on @mpapadakis varint conversion.
   * It is hard coded up to 5 bytes long, so it can support 32bit unsigned integers.
   * Anything more than that and it will blow up in your face.
   *
   * @param num the number to be transformed
   * @return the varint
   */
  public static byte[] getVarInt(long num) throws TankException {
    if (num > UINT32_MAX) {
      throw new TankException("Number Too Large (max " + UINT32_MAX + "): " + num);
    }

    byte[] result = new byte[0];
    if (num < (1 << VARINT_BYTE_SHIFT_ONE)) {
      result = new byte[1];
      result[0] = (byte)num;
    } else if (num < (1 << VARINT_BYTE_SHIFT_TWO)) {
      result = new byte[2];
      result[0] = asFlipped(num);
      result[1] = (byte)(num >> VARINT_BYTE_SHIFT_ONE);
    } else if (num < (1 << VARINT_BYTE_SHIFT_THREE)) {
      result = new byte[3];
      result[0] = asFlipped(num);
      result[1] = asFlipped(num >> VARINT_BYTE_SHIFT_ONE);
      result[2] = (byte)(num >> VARINT_BYTE_SHIFT_TWO);
    } else if (num < (1 << VARINT_BYTE_SHIFT_FOUR)) {
      result = new byte[4];
      result[0] = asFlipped(num);
      result[1] = asFlipped(num >> VARINT_BYTE_SHIFT_ONE);
      result[2] = asFlipped(num >> VARINT_BYTE_SHIFT_TWO);
      result[3] = (byte)(num >> VARINT_BYTE_SHIFT_THREE);
    } else {
      result = new byte[5];
      result[0] = asFlipped(num);
      result[1] = asFlipped(num >> VARINT_BYTE_SHIFT_ONE);
      result[2] = asFlipped(num >> VARINT_BYTE_SHIFT_TWO);
      result[3] = asFlipped(num >> VARINT_BYTE_SHIFT_THREE);
      result[4] = (byte)(num >> VARINT_BYTE_SHIFT_FOUR);
    }
    return result;
  }

  /**
   * Returns a String using the str8 notation.
   */
  public ByteBuffer getStr8() {
    int length = asInt(input.get());
    ByteBuffer str8 = ByteBuffer.allocate(length);

    for (int i = 0; i < length ; i++) {
      str8.put(input.get());
    }

    return str8;
  }

  /**
   * Returns a byte array in str8 notation of the given String.
   *
   * @param data the string to encode
   */
  public static byte[] getStr8(String data) throws TankException, UnsupportedEncodingException {
    return getStr8(data.getBytes("ASCII"));
  }

  /**
   * Returns a byte array in str8 notation of the given byte[].
   *
   * @param data the byte[] to encode
   */
  public static byte[] getStr8(byte[] data) throws TankException {
    if (data.length > BYTE_MAX) {
      throw new TankException("Str8 too long (max " + BYTE_MAX + " chars): " + new String(data));
    }
    int length = data.length;
    byte [] out = new byte[length + 1];
    out[0] = (byte)length;
    int pos = 1;
    for (byte b : data) {
      out[pos++] = b;
    }
    return out;
  }

  /**
   * Returns the amount of unprocessed bytes left.
   */
  public int getRemainingLength() {
    return input.remaining();
  }

  /**
   * Flushes processed bytes from ByteBuffer.
   */
  public void flushOffset() {
    this.flushOffset(false);
  }

  /**
   * Flushes processed bytes from ByteBuffer.
   *
   * @param storePosition instructs it to store position. i.e. when called internally.
   */
  private void flushOffset(boolean storePosition) {
    if (storePosition) {
      storedPos += getOffset();
    } else {
      storedPos = 0;
    }
    this.input = input.slice();
    totalLength = input.remaining();
  }

  /**
   * Returns the current count of processed bytes since last Flush.
   */
  public int getOffset() {
    return storedPos + input.position();
  }

  /**
   * Rewinds ByteBuffer.
   */
  public void resetOffset() {
    input.rewind();
  }

  private ByteBuffer input;
  private int totalLength;
  private int storedPos = 0;
  private static final byte VARINT_BYTE_SHIFT_ONE = 7;
  private static final byte VARINT_BYTE_SHIFT_TWO = 14;
  private static final byte VARINT_BYTE_SHIFT_THREE = 21;
  private static final byte VARINT_BYTE_SHIFT_FOUR = 28;
  private static final byte VARINT_BYTE_MAX = 127;
  private static final int BYTE_MAX = 255;
  private static final long UINT32_MAX = 4294967295L;
}
