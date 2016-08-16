package gr.phaistosnetworks.TANK;

import java.io.*;
import java.nio.*;
import org.xerial.snappy.Snappy;

//http://www.java2s.com/Code/Java/Collections-Data-Structure/ConvertbytearraytoIntegerandLong.htm
//http://www.tutorialspoint.com/java/java_bitwise_operators_examples.htm

public class ByteManipulator {
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
        long shift = 0l;

        for (int i=0; i < len; i++) {
            shift = i * Byte.SIZE;
            output[i] = (byte) (data >> shift);
        }
        return output;
    }

    public byte[] get(int length) {
        byte bar[] = new byte[length];
        for (int i=0; i<length; i++) bar[i] = input[offset+i];
        offset += length;
        return bar;
    }

    public void append(byte[] in) {
        byte souma[] = new byte[input.length + in.length];

        for (int i=0;i<input.length;i++) souma[i] = input[i];
        for (int i=0;i<in.length;i++) souma[input.length+i] = in[i];

        input = souma;
    }

    public long deSerialize(int length) {
        int len = length / Byte.SIZE;
        long result = 0l;

        for (int i = 0, n = 0; i != len; ++i, n += 8) {
            long mask = input[offset + i] & 0xff;
            result |= (mask << n);
        }

        offset += len;
        return result;
    }

    private int flipped(byte v) {
        return as_int(v & ~(1<<7));
    }

    private int as_int(int v) {
        return v<0 ? (v+256) : v;
    }

    public long getVarInt() {
        long result = 0;
        int len = 0;

        if (as_int(input[offset]) > 127) {
            if (as_int(input[offset+1]) > 127) {
                if (as_int(input[offset+2]) > 127) {
                    if (as_int(input[offset+3]) > 127) {
                        len = 5;
                        result |= flipped(input[offset]) | (flipped(input[offset+1]) << 7) | (flipped(input[offset+2]) << 14) | (flipped(input[offset+3]) << 21) | (as_int(input[offset+4]) << 28); 
                    } else {
                        len = 4;
                        result |= flipped(input[offset]) | (flipped(input[offset+1]) << 7) | (flipped(input[offset+2]) << 14) | (as_int(input[offset+3]) << 21);
                    }
                } else {
                    len = 3;
                    result |= flipped(input[offset]) | (flipped(input[offset+1]) << 7) | (as_int(input[offset+2]) << 14);
                }
            } else {
                len = 2;
                result |= flipped(input[offset]) | (as_int(input[offset+1]) << 7);
            }
        } else {
            result |= as_int(input[offset]);
            len = 1;
        }

/*
        System.out.println();
        for (int l=0; l<len; l++)
            System.out.format("Byte Seq: %d - orig:%d as_int:%d flipped:%d\n",l,  input[offset+l], as_int(input[offset + l]), flipped(input[offset+l]));
*/

        offset += len;

        return result;
    }

    private byte as_flipped(long v) {
        return (byte)(v | (1<<7));
    }

    public byte[] getVarInt(long n) {
        byte[] result = new byte[0];
        if (n < (1 << 7)) {
            result = new byte[1];
            result[0] = (byte)n;
        } else if (n < (1 << 14)) {
            result = new byte[2];
            result[0] = as_flipped(n);
            result[1] = (byte)(n >> 7);
        } else if (n < (1 << 21)) {
            result = new byte[3];
            result[0] = as_flipped(n);
            result[1] = as_flipped(n >> 7);
            result[2] = (byte)(n >> 14);
        } else if (n < (1 << 28)) {
            result = new byte[4];
            result[0] = as_flipped(n);
            result[1] = as_flipped(n >> 7);
            result[2] = as_flipped(n >> 14);
            result[3] = (byte)(n >> 21);
        } else {
            result = new byte[5];
            result[0] = as_flipped(n);
            result[1] = as_flipped(n >> 7);
            result[2] = as_flipped(n >> 14);
            result[3] = as_flipped(n >> 21);
            result[4] = (byte)(n >> 28);
        }
        return result;
    }

    public String getStr8() {
        short len = input[offset];
        offset++;

        byte op[] = new byte[len];
        for (int i=0; i < len; i++) op[i] = input[offset+i];
        offset += len;
        return new String(op);
    }

    public byte[] getStr8(String data) {
        byte len = (byte)(data.length());
        byte out[] = new byte[len+1];
        out[0]=len;
        int i=1;
        try {
            for (byte b : data.getBytes("ASCII")) out[i++] = b;
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            System.exit(1);
        }
        return out;
    }

    public int getRemainingLength() {
        return input.length - offset;
    }

    public byte[] unCompress(long len) throws IOException {
        byte toDC[] = new byte[(int)len];
        for (int i=0; i<len; i++) toDC[i] = input[offset+i];
        offset += len;
        return Snappy.uncompress(toDC);
    }

    public void flushOffset() {
        byte newInput[] = new byte[getRemainingLength()];
        for (int i=0;i < getRemainingLength() ; i++) newInput[i] = input[offset+i];
        input = newInput;
        offset = 0;
    }

    public int getOffset() {
        return offset;
    }

    public void resetOffset() {
        offset = 0;
    }

    private byte input[];
    private int offset;
}
