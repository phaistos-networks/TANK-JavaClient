import java.io.*;
import java.nio.*;

//http://www.java2s.com/Code/Java/Collections-Data-Structure/ConvertbytearraytoIntegerandLong.htm
//http://www.tutorialspoint.com/java/java_bitwise_operators_examples.htm

class ByteManipulator {
	public ByteManipulator() {
		offset = 0;
	}

	public ByteManipulator(byte[] in) { 
		input = in; 
		offset = 0;
	}

	public byte[] serialize(long data, int length) {
		int len = length / Byte.SIZE;
		byte[] output = new byte[len];
		int shift = 0;
		for (int i=0; i < len; i++) {
			shift = i * Byte.SIZE;
			output[i] = (byte) (data >> shift);
		}
		return output;
	}

	public void skip(int length) {
		offset += length;
	}

	public byte[] get(int length) {
		byte bar[] = new byte[length];
		for (int i=0; i<length; i++)
			bar[i] = input[offset+i];
		offset += length;
		return bar;
	}

	public long deSerialize(byte[] in, int length) {
		input = in;
		offset = 0;
		return deSerialize(length);
	}

	public long deSerialize(int length) {
		int len = length / Byte.SIZE;
		long result = 0;
		for (int i=0; i<len; i++) {
		//	System.out.format("byte:%d o:%d ", input[offset + len -1 -i], offset + len -1 -i);
			result |= (input[offset + len -1 -i] & 0xFF);
		}
		offset += len;
		return result;
		
	}

        private byte flipped(byte v) {
                return (v &= ~128);
        }

        long getVarInt() {
		long result = 0;
                if (input[offset] > 127) {
                        if (input[offset+1] > 127) {
                                if (input[offset+2] > 127) {
                                        if (input[offset+3] > 127) {
                                                result |= flipped(input[offset]) | (flipped(input[offset+1]) << 7) | (flipped(input[offset+2]) << 14) | (flipped(input[offset+3]) << 21) | (input[offset+4] << 28);                     
                                                offset += 5;
                                        } else {
                                                result |= flipped(input[offset]) | (flipped(input[offset+1]) << 7) | (flipped(input[offset+2]) << 14) | (input[offset+3] << 21);
                                                offset += 4;
                                        }
                                } else {
                                        result |= flipped(input[offset]) | (flipped(input[offset+1]) << 7) | (input[offset+2] << 14);
                                        offset += 3;
                                }
                        } else {
                                result |= flipped(input[offset]) | (input[offset+1] << 7);
                                offset += 2;
                        }
                } else
                        result |= input[offset+0];
			offset ++;

		return result;
        }

	String getStr8() {
		short len = input[offset];
		offset++;

		byte op[] = new byte[len];
		for (int i=0; i < len; i++)
			op[i] = input[offset+i];
		offset += len;
		return new String(op);
	}

	public byte[] getStr8(String data) {
                byte len = (byte)(data.length());
                byte out[] = new byte[len+1];
                out[0]=len;
                int i=1;
                try {
                        for (byte b : data.getBytes("ASCII")) {
                                out[i++] = b;
                        }
                } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                        System.exit(1);
                }
                return out;
        }

	public int getRemainingLength() {
		return input.length - offset;
	}

	private byte input[];
	private int offset;
}
