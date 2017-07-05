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
   * Constructor that sets the byte array to be manipulated.
   *
   * @param input the byte array to be manipulated;
   */
  public ByteManipulator(byte[] input) {
    this.input = input;
    this.offset = 0;
  }

  /**
   * Constructor that sets the byte array from ByteBuffer.
   *
   * @param input the ByteBuffer to be manipulated;
   */
  public ByteManipulator(ByteBuffer input, int bbLength) {
    this.input = new byte[bbLength];
    input.get(this.input);
    this.offset = 0;
  }

  /**
   * Appends a byte array to the current one.
   *
   * @param toAppend the byte array to append.
   */
  public void append(byte[] toAppend) {
    byte [] souma = new byte[input.length + toAppend.length];

    for (int i = 0; i < input.length; i++) {
      souma[i] = input[i];
    }

    for (int i = 0; i < toAppend.length; i++) {
      souma[input.length + i] = toAppend[i];
    }

    this.input = souma;
  }

  /**
   * Appends a ByteBuffer to the current Byte array.
   *
   * @param toAppend the ByteBuffer to append.
   */
  public void append(ByteBuffer toAppend, int bbLength) {
    byte [] souma = new byte[input.length + bbLength];

    for (int i = 0; i < input.length; i++) {
      souma[i] = input[i];
    }

    for (int i = 0; i < bbLength; i++) {
      souma[input.length + i] = toAppend.get();
    }

    this.input = souma;
  }

  /**
   * Returns the next length bytes from the current byte array.
   * There is no health check. If you request more bytes than available, boom
   *
   * @param length the amount of bytes to return
   * @return a byte array containing the requested length of bytes
   */
  public byte[] getNextBytes(int length) {
    byte [] bar = new byte[length];
    for (int i = 0; i < length; i++) {
      bar[i] = input[offset + i];
    }
    offset += length;
    return bar;
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
    for (int i = 0; i < length; i++) {
      toUnCompress[i] = input[offset + i];
    }
    offset += length;
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
      long mask = input[offset + i] & BYTE_MAX;
      result |= (mask << n);
    }

    offset += length;
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
   * Reads a varint from the next unprocessed bytes of the current array.
   *
   * @return the value of the varint
   */
  public long getVarInt() {
    long result = 0;
    int length = 0;

    if (asInt(input[offset]) > VARINT_BYTE_MAX) {
      if (asInt(input[offset + 1]) > VARINT_BYTE_MAX) {
        if (asInt(input[offset + 2]) > VARINT_BYTE_MAX) {
          if (asInt(input[offset + 3]) > VARINT_BYTE_MAX) {
            length = 5;
            result |= flipped(input[offset])
              | (flipped(input[offset + 1]) << VARINT_BYTE_SHIFT_ONE)
              | (flipped(input[offset + 2]) << VARINT_BYTE_SHIFT_TWO)
              | (flipped(input[offset + 3]) << VARINT_BYTE_SHIFT_THREE)
              | (asInt(input[offset + 4]) << VARINT_BYTE_SHIFT_FOUR);
          } else {
            length = 4;
            result |= flipped(input[offset])
              | (flipped(input[offset + 1]) << VARINT_BYTE_SHIFT_ONE)
              | (flipped(input[offset + 2]) << VARINT_BYTE_SHIFT_TWO)
              | (asInt(input[offset + 3]) << VARINT_BYTE_SHIFT_THREE);
          }
        } else {
          length = 3;
          result |= flipped(input[offset])
            | (flipped(input[offset + 1]) << VARINT_BYTE_SHIFT_ONE)
            | (asInt(input[offset + 2]) << VARINT_BYTE_SHIFT_TWO);
        }
      } else {
        length = 2;
        result |= flipped(input[offset])
          | (asInt(input[offset + 1]) << VARINT_BYTE_SHIFT_ONE);
      }
    } else {
      result |= asInt(input[offset]);
      length = 1;
    }

    offset += length;
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
  public String getStr8() {
    int length = asInt(input[offset]);
    offset++;

    byte [] op = new byte[length];
    for (int i = 0; i < length; i++) {
      op[i] = input[offset + i];
    }
    offset += length;
    return new String(op);
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
    return input.length - offset;
  }

  /**
   * Flushes processed bytes from byte array.
   */
  @Deprecated
  public void flushOffset() {
    offsetMark = offset;
    byte [] newInput = new byte[getRemainingLength()];
    for (int i = 0; i < getRemainingLength(); i++) {
      newInput[i] = input[offset + i];
    }
    input = newInput;
    offset = 0;
  }

  /**
   * Retuns the current count of processed bytes.
   */
  public int getOffset() {
    return offset;
  }

  /**
   * Marks the current offset position
   */
  public void markOffset() {
    offsetMark = offset;
  }

  /**
   * Returns the current count of processed bytes since the Mark.
   */
  public int getMarkedOffset() {
    return offset - offsetMark;
  }

  /**
   * Resets current processed byte counter.
   */
  public void resetOffset() {
    offset = 0;
    offsetMark = 0;
  }

  /**
   * prints all bytes in current byte array.
   */
  public void printBytes() {
    offset = 0;
    for (int i = 0 ; i < input.length ; i++) {
      System.out.format("%d ", input[i]);
    }
  }

  private byte [] input;
  private int offset;
  private int offsetMark;
  private static final byte VARINT_BYTE_SHIFT_ONE = 7;
  private static final byte VARINT_BYTE_SHIFT_TWO = 14;
  private static final byte VARINT_BYTE_SHIFT_THREE = 21;
  private static final byte VARINT_BYTE_SHIFT_FOUR = 28;
  private static final byte VARINT_BYTE_MAX = 127;
  private static final int BYTE_MAX = 255;
  private static final long UINT32_MAX = 4294967295L;
}
