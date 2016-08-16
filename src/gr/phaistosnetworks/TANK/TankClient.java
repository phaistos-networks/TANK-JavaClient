package gr.phaistosnetworks.TANK;

import java.net.*;
import java.io.*;
import java.nio.*;
import java.util.ArrayList;
import java.util.logging.*;

/**
 * TankClient implementation for java
 *
 * @author Robert Krambovitis @rkrambovitis
 */
public class TankClient {

    /**
     * Constructor for the TankClient object
     * Upon initialization TankClient will attempt to connect to server.
     * Upon success, it will check for ping response from server.
     * If it's unsuccesful and error will be logged, and it will retry indefinately.
     *
     * @param tHost tank host to connect to
     * @param tPort tank port to connect to
     */
    public TankClient(String tHost, int tPort) {
        tankHost = tHost;
        tankPort = tPort;
        clientReqID = 0;
        log = Logger.getLogger("tankClient");
        messages = new ArrayList<TankMessage>();
        reqType = PING_REQ;

        try {
            while (true) {
                try {
                    client = new Socket(tankHost, tankPort);
                } catch (Exception e) {
                    log.severe(e.getMessage());
                    log.log(Level.FINEST, "ERROR opening socket", e);
                    Thread.sleep(TankClient.RETRY_INTERVAL);
                    continue;
                }
                //client.setSoTimeout(1000);
                client.setTcpNoDelay(true);
                client.setKeepAlive(true);
                client.setReuseAddress(true);
                log.config("Connected to " + client.getRemoteSocketAddress());
                log.config(" + recv buffer size: " + client.getReceiveBufferSize());
                log.config(" + send buffer size: " + client.getSendBufferSize());
                log.config(" + timeout: " + client.getSoTimeout());
                log.config(" + soLinger: " + client.getSoLinger());
                log.config(" + nodelay: " + client.getTcpNoDelay());
                log.config(" + keepalive: " + client.getKeepAlive());
                log.config(" + oobinline: " + client.getOOBInline());
                log.config(" + reuseAddress: " + client.getReuseAddress());

                bis = new BufferedInputStream(client.getInputStream());
                socketOutputStream = client.getOutputStream();
                break;
            }
            while (true) {
                if (!getPing(bis)) log.severe("ERROR: No Ping Received");
                else {
                    log.fine("PING OK");
                    break;
                }
            }
        } catch (Exception e) {
            log.log(Level.SEVERE, "ERROR opening Streams", e);
            System.exit(1);
        }
    }

    /**
     * performs a consume request from server and returns the data
     *
     * @param topic the topic to consume from
     * @param partition the partition to consume from
     * @param rSeqNum the sequence number to start consuming from.
     * @return an ArrayList<TankMessage> containing the response data.
     */
    public ArrayList<TankMessage> consume(String topic, int partition, long rSeqNum) throws IOException, TankException {
        log.fine("Received consume req seq: " + rSeqNum + " reqID: " + (clientReqID + 1));
        reqType = TankClient.CONSUME_REQ;

        Topic topics[] = new Topic[1];
        topics[0] = new Topic(topic, partition, rSeqNum, 20000L);

        byte req[] = fetchReq(0L, clientReqID++, "java", 1000L, 0L, topics);
        byte rsize[] = (ByteManipulator.serialize(req.length - U8 - U32, U32));
        for (int i = 0; i < 4; i++) req[i + 1] = rsize[i];
        socketOutputStream.write(req);
        poll();
        return messages;
    }

    /**
     * tries to get data from the network socket.
     * It will loop and retry if there's no data.
     * If the data is incomplete, i.e. available less than payload, 
     * then it will loop and append until payload size is reached.
     * if the request was a consume, it will populate ArrayList<TankMessage> messages.
     * else if it was a publish, it will eat fried monkey brains.
     */
    private void poll() throws IOException, TankException {
        messages = new ArrayList<TankMessage>();
        ByteManipulator input = new ByteManipulator(null);
        int remainder = 0;
        int toRead = 0;
        while (true) {
            int av = bis.available();
            log.finest("bytes available: " + av);
            if (av == 0) {
                try {
                    Thread.sleep(TankClient.RETRY_INTERVAL);
                } catch (InterruptedException e) {
                    log.warning("Cmon, lemme get some sleep ! " + e.getCause());
                } finally {
                    continue;
                }
            }

            if (remainder >= 0) toRead = av;
            else toRead = remainder;

            byte ba[] = new byte[toRead];
            bis.read(ba, 0, toRead);

            if (remainder > 0) input.append(ba);
            else input = new ByteManipulator(ba);

            byte resp = (byte)input.deSerialize(U8);
            long payloadSize = input.deSerialize(U32);

            if (resp != reqType) {
                log.severe("Bad Response type. Expected " + reqType + ", got " + resp);
                throw new TankException("Bad Response type. Expected " + reqType + ", got " + resp);
            }

            if (payloadSize > input.getRemainingLength()) {
                log.warning("Received packet incomplete ");
                remainder = (int)(payloadSize - input.getRemainingLength());
                input.resetOffset();
                continue;
            } else remainder = 0;

            log.fine("resp: " + resp);
            log.fine("payload size: " + payloadSize);

            if (reqType == TankClient.CONSUME_REQ) getMessages(input);
            else if (reqType == TankClient.PUBLISH_REQ) getPubResponse(input);

            for (Handler h : log.getHandlers()) h.flush();
            break;
        }
    }

    /**
     * Processes the input received from tank server.
     * Populates ArrayList<TankMessage> messages.
     *
     * @param input the ByteManipulator object that contains the bytes received from server.
     */
    private void getMessages(ByteManipulator input) {
        ArrayList<Chunk> chunkList = new ArrayList<Chunk>();
        //ArrayList<TankMessage> messages = new ArrayList<TankMessage>();
        // Headers
        long headerSize  = input.deSerialize(U32);
        long reqId = input.deSerialize(U32);
        long totalTopics = input.deSerialize(U8);
        log.fine("header size: " + headerSize);
        log.fine("reqid: " + reqId);
        log.fine(String.format("topics count: %d\n", totalTopics));

        for (int t = 0; t < totalTopics; t++) {
            String topic = input.getStr8();
            long totalPartitions = input.deSerialize(U8);
            log.fine("topic name: " + topic);
            log.fine("Total Partitions: " + totalPartitions);

            int partition = (int)input.deSerialize(U16);
            if (partition == 65535) {
                log.warning("Topic " + topic + " Not Found ");
                continue;
            } else {
                // Partitions
                for (int p = 0; p < totalPartitions; p++) {
                    if (p != 0) partition = (int)input.deSerialize(U16);

                    byte errorOrFlags = (byte)input.deSerialize(U8);
                    log.fine("Partition : " + partition);
                    log.fine(String.format("ErrorOrFlags : %x\n", errorOrFlags));

                    if ((errorOrFlags & 0xFF) == 0xFF) {
                        log.warning("Unknown Partition");
                        continue;
                    }

                    long baseAbsSeqNum = 0L;
                    if ((errorOrFlags & 0xFF) != 0xFE) {
                        baseAbsSeqNum = input.deSerialize(U64);
                        log.fine("Base Abs Seq Num : " + baseAbsSeqNum);
                    }

                    long highWaterMark = input.deSerialize(U64);
                    long chunkLength = input.deSerialize(U32);
                    log.fine("High Watermark : " + highWaterMark);
                    log.fine("Chunk Length : " + chunkLength);

                    if (errorOrFlags == 0x1) {
                        long firstAvailSeqNum = input.deSerialize(U64);
                        log.warning("Sequence out of bounds. Range: " + firstAvailSeqNum + " - " + highWaterMark);
                        continue;
                    }
                    chunkList.add(new Chunk(topic, partition, errorOrFlags, baseAbsSeqNum, highWaterMark, chunkLength));
                }
            }
        }

        //Chunks
        long curSeqNum = 0;
        for (Chunk c : chunkList) {
            while (input.getRemainingLength() > 0) {
                log.finer("Remaining Length: " + input.getRemainingLength());
                long bundleLength = input.getVarInt();
                log.finer("Bundle length : " + bundleLength);
                if (bundleLength > input.getRemainingLength()) {
                    log.fine("Bundle Incomplete (remaining bytes: " + input.getRemainingLength() + ")");
                    return;
                }
                input.flushOffset();

                byte flags = (byte)input.deSerialize(U8);
                long messageCount = (flags >> 2) & 0xf;
                long compressed = flags & 0x3;
                long sparse = (flags >> 6) & 0xf;
                log.finer("Bundle compressed : " + compressed);
                log.finer("Bundle SPARSE : " + sparse);

                if (messageCount == 0) messageCount = input.getVarInt();
                log.finer("Messages in set : " + messageCount);

                long firstMessageNum = 0L;
                long lastMessageNum = 0L;
                if (sparse == 1) {
                    firstMessageNum = input.deSerialize(U64);
                    log.finer("First message: " + firstMessageNum);
                    if (messageCount > 1) {
                        lastMessageNum = input.getVarInt() + 1 + firstMessageNum;
                        log.finer("Last message: " + lastMessageNum);
                    }
                }

                ByteManipulator chunkMsgs;
                if (compressed == 1) {
                    log.finer("Snappy uncompression");
                    try {
                        chunkMsgs = new ByteManipulator(input.snappyUncompress(bundleLength - input.getOffset()));
                    } catch (IOException e) {
                        log.log(Level.SEVERE, "ERROR uncompressing", e);
                        return;
                    }
                } else chunkMsgs = input;

                long timestamp = 0L;
                long prevSeqNum = firstMessageNum;
                for (int i = 0; i < messageCount; i++) {
                    if (curSeqNum == 0) curSeqNum = c.baseAbsSeqNum - 1;
                    log.finer("#### Message " + (i + 1) + " out of " + messageCount);
                    flags = (byte)chunkMsgs.deSerialize(U8);
                    log.finer(String.format("flags : %d", flags));
                    if (sparse == 1) {
                        if (i == 0) log.finer("seq num: " + firstMessageNum);
                        else if (i != 0 && i != (messageCount - 1)) {
                            curSeqNum = (chunkMsgs.getVarInt() + 1 + prevSeqNum);
                            log.finer("seq num: " + curSeqNum);
                            prevSeqNum = curSeqNum;
                        } else log.finer("seq num: " + lastMessageNum);
                    } else curSeqNum++;
                    log.finer("cur seq num: " + curSeqNum);

                    if ((flags & USE_LAST_SPECIFIED_TS) == 0) {
                        timestamp = chunkMsgs.deSerialize(U64);
                        log.finer("New Timestamp : " + timestamp);
                    } else log.finer("Using last Timestamp : " + timestamp);

                    if ((flags & HAVE_KEY) == 1) {
                        String key = chunkMsgs.getStr8();
                        log.finer("We have a key and it is : " + key);
                    }

                    long contentLength = chunkMsgs.getVarInt();
                    log.finer("Content Length: " + contentLength);

                    byte message[] = chunkMsgs.get((int)contentLength);
                    log.finest(new String(message));
                    messages.add(new TankMessage(curSeqNum, timestamp, message));
                }
            }
        }
    }

    /**
     * publishes an ArrayList<byte[]> of messages to tank server.
     *
     * @param topic the topic to publish to
     * @param partition the partition to publish to
     * @param msgs the ArrayList of messages to publish
     */
    public void publish(String topic, int partition, ArrayList<byte[]> msgs) throws IOException, TankException {
        log.fine("Received pub req with " + msgs.size() + " messages");
        reqType = TankClient.PUBLISH_REQ;

        Topic topics[] = new Topic[1];
        Bundle bun = new Bundle(msgs);

        topics[0] = new Topic(topic, partition, bun);
        byte req[] = publishReq(0L, clientReqID++, "java", 0, 0L, topics);
        byte rsize[] = (ByteManipulator.serialize(req.length - 5, U32));
        for (int i = 0; i < 4; i++) req[i + 1] = rsize[i];

        socketOutputStream.write(req);
        poll();
    }

    /**
     * Processes a response from tank server after we issue a publish request.
     *
     * @param input a ByteManipulator object containing the data received from server.
     */
    private void getPubResponse(ByteManipulator input) throws TankException {
        log.fine("request ID: " + input.deSerialize(U32));
        long error = input.deSerialize(U8);
        if (error == 0xff) {
            log.fine("Topic Error: " + error);
            log.severe("Error, Topic not found");
            throw new TankException("Topic Error: " + error);
        } else if (error != 0) throw new TankException("Partition Error: " + error);
        else log.fine("Partition Error: " + error);
    }

    /**
     * Create the encoded fetch request
     *
     * @param clientVersion optional version number
     * @param reqID optional request id
     * @param cliendID option client ID
     * @param maxWait See tank_protocol.md for maxWait semantics.
     * @param minBytes see tank_protocol.md for minBytes semantics.
     * @param topic array of topics to be serialized.
     * @return a byte array containing the request to be sent to TANK. See tank_protocol.md for details.
     */
    private byte[] fetchReq(long clientVersion, long reqID, String clientId, long maxWait, long minBytes, Topic[] topics) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            baos.write((byte)0x2);
            baos.write(ByteManipulator.serialize(0, U32));
            baos.write(ByteManipulator.serialize(clientVersion, U16));
            baos.write(ByteManipulator.serialize(reqID, U32));
            baos.write(ByteManipulator.getStr8(clientId));
            baos.write(ByteManipulator.serialize(maxWait, U64));
            baos.write(ByteManipulator.serialize(minBytes, U32));
            baos.write(ByteManipulator.serialize(topics.length, U8));
            for (int i = 0; i < topics.length; i++) baos.write(topics[i].serialize());
        } catch (Exception e) {
            log.log(Level.SEVERE, "ERROR creating fetch request", e);
            System.exit(1);
        }
        return baos.toByteArray();
    }

    /**
     * Create the encoded publish request
     *
     * @param clientVersion optional version number
     * @param reqID optional request id
     * @param cliendID option client ID
     * @param reqAcks number of required acks. Used in clustered mode setups.
     * @param ackTimeout timeout for acks. Used in clustered mode setups.
     * @param topic array of topics to be serialized.
     * @return a byte array containing the request to be sent to TANK. See tank_protocol.md for details.
     */
    private byte[] publishReq(long clientVersion, long reqID, String clientId, int reqAcks, long ackTimeout, Topic[] topics) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            baos.write((byte)0x1);
            baos.write(ByteManipulator.serialize(0, U32));

            baos.write(ByteManipulator.serialize(clientVersion, U16));
            baos.write(ByteManipulator.serialize(reqID, U32));
            baos.write(ByteManipulator.getStr8(clientId));

            baos.write(ByteManipulator.serialize(reqAcks, U8));
            baos.write(ByteManipulator.serialize(ackTimeout, U32));

            baos.write(ByteManipulator.serialize(topics.length, U8));
            for (int i = 0; i < topics.length; i++) baos.write(topics[i].serialize());

        } catch (Exception e) {
            log.log(Level.SEVERE, "ERROR creating publish request", e);
            System.exit(1);
        }
        return baos.toByteArray();
    }

    /**
     * get a valid ping response from server.
     * 
     * @param bis BufferedInputStream to read from
     * @return true if valid ping response is received.
     */
    private boolean getPing(BufferedInputStream bis) {
        try {
            int av = bis.available();
            if (av == 0) return false;

            byte b = (byte)bis.read();
            if (b != 0x3) return false;
            // payload size:u32 is 0 for ping
            bis.skip(4);
        } catch (Exception e) {
            log.log(Level.SEVERE, "ERROR getting ping", e);
            return false;
        }
        return true;
    }

    /**
     * see tank_protocol.md for chunk details.
     */
    private class Chunk {
        /**
         * constructor
         *
         * @param t topic
         * @param p partition
         * @param eof errorOrFlags
         * @param basn base absolute sequence number.
         * @param hwm high water mark
         * @param l length
         */
        public Chunk(String t, int p, byte eof, long basn, long hwm, long l) {
            topic = t;
            partition = p;
            errorOrFlags = eof;
            baseAbsSeqNum = basn;
            highWaterMark = hwm;
            length = l;
        }
        private String topic;
        private long partition;
        private byte errorOrFlags;
        private long highWaterMark;
        private long length;
        private long baseAbsSeqNum;
    }

    /**
     * Implementation of topics used in pub and consume requests.
     */
    private class Topic {
        /**
         * constructor for topic used in fetch request.
         *
         * @param name topic name
         * @param partition partition id
         * @param seqNum request sequence number
         * @param fetchSize see TANK tank_protocol.md for fetch fize semantics
         */
        private Topic(String name, int partition, long seqNum, long fetchSize) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try {
                baos.write(ByteManipulator.getStr8(name));
                baos.write(ByteManipulator.serialize(1L, U8));
                baos.write(ByteManipulator.serialize(partition, U16));
                baos.write(ByteManipulator.serialize(seqNum, U64));
                baos.write(ByteManipulator.serialize(fetchSize, U32));
            } catch (IOException e) {
                log.log(Level.SEVERE, "ERROR creating Topic", e);
                System.exit(1);
            }
            topic = baos.toByteArray();
        }

        /**
         * constructor for topic used in publish request.
         *
         * @param name topic name
         * @param partition partition id
         * @param bun bundle to publish. See TANK tank_encoding.md
         */
        private Topic(String name, int partition, Bundle bun) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try {
                baos.write(ByteManipulator.getStr8(name));
                baos.write(ByteManipulator.serialize(1L, U8));
                baos.write(ByteManipulator.serialize(partition, U16));
                byte[] bb = bun.serialize();
                baos.write(ByteManipulator.getVarInt(bb.length));
                baos.write(bb);
            } catch (IOException e) {
                log.log(Level.SEVERE, "ERROR creating Topic", e);
                System.exit(1);
            }
            topic = baos.toByteArray();
        }

        /**
         * serialize topic into array of bytes
         *
         * @return properly encoded byte[]
         */
        public byte[] serialize() {
            return topic;
        }

        private byte topic[];
    }

    /**
     * see tank_encoding.md for chunk details.
     */
    private class Bundle {
    /*
        private Bundle() {
            messages = new ArrayList<TankMessage>();
        }
        */

        /**
         * constructor for new bundle given ArrayList of byte[] messages
         *
         * @param msgs the messages to be included in the bundle
         */
        private Bundle(ArrayList<byte[]> msgs) {
            messages = new ArrayList<TankMessage>();
            for (byte[] m : msgs) addMsg(new TankMessage(0L, m));
        }

        /**
         * add TankMessage to bundle
         *
         * @param tm tankMessage to add
         */
        void addMsg(TankMessage tm) {
            messages.add(tm);
        }


        /**
         * serialize bundle into array of bytes
         *
         * @return properly encoded byte[]
         */
        public byte[] serialize() {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try {
                byte flags = 0;
                if (messages.size() <= 15) flags |= (messages.size() << 2);

                baos.write(ByteManipulator.serialize(flags, U8));

                if (messages.size() > 15) baos.write(ByteManipulator.getVarInt(messages.size()));

                flags = 0;
                for (TankMessage tm : messages) baos.write(tm.serialize(flags, ""));
            } catch (IOException e) {
                log.log(Level.SEVERE, "ERROR creating Topic", e);
                System.exit(1);
            }
            return baos.toByteArray();
        }
        private ArrayList<TankMessage> messages;
    }

    private String tankHost;
    private int tankPort;
    private int reqSeqNum;
    private Socket client;
    private BufferedInputStream bis;
    private OutputStream socketOutputStream;
    private Logger log;
    private int clientReqID;
    private ArrayList<TankMessage> messages;
    private short reqType;

    public static final byte HAVE_KEY = 1;
    public static final byte USE_LAST_SPECIFIED_TS = 2;
    public static final long RETRY_INTERVAL = 50;

    public static final short PUBLISH_REQ = 1;
    public static final short CONSUME_REQ = 2;
    public static final short PING_REQ = 3;

    public static final byte U8 = 8;
    public static final byte U16 = 16;
    public static final byte U32 = 32;
    public static final byte U64 = 64;
}