import java.net.*;
import java.io.*;
import java.nio.*;
import java.util.ArrayList;
import java.util.logging.*;

class TankClient {
        public TankClient(String tHost, int tPort) {
                tankHost = tHost;
                tankPort = tPort;
		clientReqID = 0;
		log = Logger.getLogger("tankClient");
		gandalf = new ByteManipulator();
		messages = new ArrayList<TankMessage>();
		reqType = 3;

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
				log.config("Connected to "+ client.getRemoteSocketAddress());
				log.config(" + recv buffer size: "+ client.getReceiveBufferSize());
				log.config(" + send buffer size: "+ client.getSendBufferSize());
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
				if ( !getPing(bis) ) log.severe("ERROR: No Ping Received");
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

        public ArrayList<TankMessage> consume(String topic, int partition, long rSeqNum) throws IOException, TankException {
		log.fine("Received consume req seq: "+rSeqNum+" reqID: "+(clientReqID+1));
		reqType=TankClient.CONSUME_REQ;

		Topic topics[] = new Topic[1];
		topics[0] = new Topic(topic, partition, rSeqNum, 20000l);

		byte req[] = fetchReq(0l, clientReqID++, "java", 1000l, 0l, topics);
		byte rsize[] = (gandalf.serialize(req.length-5, 32));
		for (int i=0; i<4; i++) req[i+1] = rsize[i];
		socketOutputStream.write(req);
		poll();
		return messages;
	}

	private void poll() throws IOException, TankException {
		messages = new ArrayList<TankMessage>();
		ByteManipulator input = new ByteManipulator();
		int remainder = 0;
		int toRead = 0;
		while (true) {
			int av = bis.available();
			log.finest("bytes available: "+av);
			if (av == 0) {
				try {
					Thread.sleep(TankClient.RETRY_INTERVAL);
				} catch (InterruptedException e) {
					log.warning("Cmon, lemme get some sleep ! "+e.getCause());
				} finally {
					continue;
				}
			}

			if (remainder >= 0) toRead = av;
			else
				toRead = remainder;

			byte ba[] = new byte[toRead];
			bis.read(ba, 0, toRead);

			if (remainder > 0 ) input.append(ba);
			else
				input = new ByteManipulator(ba);

			byte resp = (byte)input.deSerialize(8);
			long payloadSize = input.deSerialize(32);

			if (resp != reqType) {
				log.severe("Bad Response type. Expected "+reqType+", got "+resp);
				throw new TankException("Bad Response type. Expected "+reqType+", got "+resp);
			}

			if (payloadSize > input.getRemainingLength()) {
				log.warning("Received packet incomplete ");
				remainder = (int)(payloadSize - input.getRemainingLength());
				input.resetOffset();
				continue;
			} else
				remainder = 0;

			log.fine("resp: " + resp);
			log.fine("payload size: " + payloadSize);

			if (reqType == TankClient.CONSUME_REQ) getMessages(input);
			else if (reqType == TankClient.PUBLISH_REQ) getPubResponse(input);

			for (Handler h : log.getHandlers()) h.flush();
			break;
		}
	}

	private void getMessages(ByteManipulator input) {
		ArrayList<Chunk> chunkList = new ArrayList<Chunk>();
		//ArrayList<TankMessage> messages = new ArrayList<TankMessage>();
		// Headers
		long headerSize  = input.deSerialize(32);
		long reqId = input.deSerialize(32);
		long totalTopics = input.deSerialize(8);
		log.fine("header size: " + headerSize);
		log.fine("reqid: "+ reqId);
		log.fine(String.format("topics count: %d\n", totalTopics));

		for (int t=0 ; t<totalTopics ; t++) {
			String topic = input.getStr8();
			long totalPartitions = input.deSerialize(8);
			log.fine("topic name: " + topic);
			log.fine("Total Partitions: "+ totalPartitions);

			int partition = (int)input.deSerialize(16);
			if (partition == 65535) {
				log.warning("Topic "+topic+" Not Found ");
				continue;
			} else {
				// Partitions
				for (int p=0 ; p < totalPartitions; p++) {
					if (p != 0) partition = (int)input.deSerialize(16);

					byte errorOrFlags = (byte)input.deSerialize(8);
					log.fine("Partition : " + partition);
					log.fine(String.format("ErrorOrFlags : %x\n", errorOrFlags));

					if ((errorOrFlags & 0xFF) == 0xFF) {
						log.warning("Unknown Partition");
						continue;
					}

					long baseAbsSeqNum = 0l;
					if ((errorOrFlags & 0xFF) != 0xFE) {
						baseAbsSeqNum = input.deSerialize(64);
						log.fine("Base Abs Seq Num : " +baseAbsSeqNum);
					}

					long highWaterMark = input.deSerialize(64);
					long chunkLength = input.deSerialize(32);
					log.fine("High Watermark : " +highWaterMark);
					log.fine("Chunk Length : " +chunkLength);

					if (errorOrFlags == 0x1) {
						long firstAvailSeqNum = input.deSerialize(64);
						log.warning("Sequence out of bounds. Range: "+firstAvailSeqNum+" - "+highWaterMark);
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
				log.finer("Remaining Length: "+input.getRemainingLength());
				long bundleLength = input.getVarInt();
				log.finer("Bundle length : " +bundleLength);
				if (bundleLength > input.getRemainingLength()) {
					log.fine("Bundle Incomplete (remaining bytes: "+input.getRemainingLength()+")");
					return;
				}
				input.flushOffset();

				byte flags = (byte)input.deSerialize(8);
				long messageCount = (flags >> 2) &0xf;
				long compressed = flags &0x3;
				long sparse = (flags >> 6) &0xf;
				log.finer("Bundle compressed : " +compressed);
				log.finer("Bundle SPARSE : " +sparse);

				if (messageCount == 0) messageCount = input.getVarInt();
				log.finer("Messages in set : " +messageCount);

				long firstMessageNum = 0l;
				long lastMessageNum = 0l;
				if (sparse == 1) {
					firstMessageNum = input.deSerialize(64);
					log.finer("First message: "+ firstMessageNum);
					if (messageCount > 1) {
						lastMessageNum = input.getVarInt() + 1 + firstMessageNum;
						log.finer("Last message: " + lastMessageNum);
					}
				}

				ByteManipulator chunkMsgs;
				if (compressed == 1) {
					log.finer("Snappy uncompression");
					try {
						chunkMsgs = new ByteManipulator(input.unCompress(bundleLength-input.getOffset()));
					} catch (IOException e) {
						log.log(Level.SEVERE, "ERROR uncompressing", e);
						return;
					}
				} else {
					chunkMsgs = input;
				}

				long timestamp = 0l;
				long prevSeqNum = firstMessageNum;
				for (int i = 0; i < messageCount; i++) {
					if (curSeqNum == 0) curSeqNum = c.baseAbsSeqNum-1;
					log.finer("#### Message "+ (i+1) +" out of "+messageCount);
					flags = (byte)chunkMsgs.deSerialize(8);
					log.finer(String.format("flags : %d", flags));
					if (sparse == 1) {
						if (i == 0) log.finer("seq num: " + firstMessageNum);
						else if (i!=0 && i!=messageCount-1) {
							curSeqNum = (chunkMsgs.getVarInt() + 1 + prevSeqNum);
							log.finer("seq num: " + curSeqNum);
							prevSeqNum = curSeqNum;
						} else
							log.finer("seq num: " + lastMessageNum);
					} else
						curSeqNum++;
					log.finer("cur seq num: " + curSeqNum);

					if ((flags & UseLastSpecifiedTS) == 0) {
						timestamp = chunkMsgs.deSerialize(64);
						log.finer("New Timestamp : " + timestamp);
					} else {
						log.finer("Using last Timestamp : "+ timestamp);
					}

					if ((flags & HaveKey) == 1) {
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

        public void publish(String topic, int partition, ArrayList<byte[]> msgs) throws IOException, TankException {
		log.fine("Received pub req with "+msgs.size()+" messages");
		reqType=TankClient.PUBLISH_REQ;

		Topic topics[] = new Topic[1];
		Bundle bun = new Bundle(msgs);

		topics[0] = new Topic(topic, partition, bun);
		byte req[] = publishReq(0l, clientReqID++, "java", 0, 0l, topics);
		byte rsize[] = (gandalf.serialize(req.length-5, 32));
		for (int i=0; i<4; i++) req[i+1] = rsize[i];

		socketOutputStream.write(req);
                poll();
	}

	private void getPubResponse(ByteManipulator input) throws TankException {
		log.fine("request ID: "+input.deSerialize(32));
		long error = input.deSerialize(8);
		if (error == 0xff) {
			log.fine("Topic Error: " + error);
			log.severe("Error, Topic not found");
			throw new TankException("Topic Error: "+error);
		} else if (error != 0) throw new TankException("Partition Error: "+error);
		else
			log.fine("Partition Error: " + error);
	}

	private byte[] fetchReq(long clientVersion, long reqID, String clientId, long maxWait, long minBytes, Topic[] topics) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
			baos.write((byte)0x2);
			baos.write(gandalf.serialize(0, 32));
			baos.write(gandalf.serialize(clientVersion, 16));
			baos.write(gandalf.serialize(reqID, 32));
			baos.write(gandalf.getStr8(clientId));
			baos.write(gandalf.serialize(maxWait, 64));
			baos.write(gandalf.serialize(minBytes, 32));
			baos.write(gandalf.serialize(topics.length, 8));
			for (int i=0; i< topics.length; i++) baos.write(topics[i].serialize());
		} catch (Exception e) {
			log.log(Level.SEVERE, "ERROR creating fetch request", e);
			System.exit(1);
		}
		return baos.toByteArray();
	}

	private byte[] publishReq(long clientVersion, long reqID, String clientId, int reqAcks, long ackTimeout, Topic[] topics) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
			baos.write((byte)0x1);
			baos.write(gandalf.serialize(0, 32));

			baos.write(gandalf.serialize(clientVersion, 16));
			baos.write(gandalf.serialize(reqID, 32));
			baos.write(gandalf.getStr8(clientId));

			baos.write(gandalf.serialize(reqAcks, 8));
			baos.write(gandalf.serialize(ackTimeout, 32));

			baos.write(gandalf.serialize(topics.length, 8));
			for (int i=0; i< topics.length; i++) baos.write(topics[i].serialize());

		} catch (Exception e) {
			log.log(Level.SEVERE, "ERROR creating publish request", e);
			System.exit(1);
		}
		return baos.toByteArray();
	}



	private boolean getPing(BufferedInputStream bis) {
		try {
			int av = bis.available();
			if (av == 0) return false;

			byte b = (byte)bis.read();
			if (b != 0x3) return false;
			bis.skip(4);
		} catch (Exception e) {
			log.log(Level.SEVERE, "ERROR getting ping", e);
			return false;
		}
		return true;
	}

	private class Chunk {
		public Chunk(String t, int p, byte eof, long basn, long hwm, long l) {
			topic = t;
			partition = p;
			errorOrFlags = eof;
			baseAbsSeqNum = basn;
			highWaterMark = hwm;
			length = l;
		}
		String topic;
		long partition;
		byte errorOrFlags;
		long highWaterMark;
		long length;
		long baseAbsSeqNum;
	}

	private class Topic {
		Topic(String name,int partition, long seqNum, long fetchSize) {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			try {
				baos.write(gandalf.getStr8(name));
				baos.write(gandalf.serialize(1l, 8));
				baos.write(gandalf.serialize(partition, 16));
				baos.write(gandalf.serialize(seqNum, 64));
				baos.write(gandalf.serialize(fetchSize, 32));
			} catch (IOException e) {
				log.log(Level.SEVERE, "ERROR creating Topic", e);
				System.exit(1);
			}
			topic = baos.toByteArray();
		}

		Topic(String name, int partition, Bundle bun) {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			try {
				baos.write(gandalf.getStr8(name));
				baos.write(gandalf.serialize(1l, 8));
				baos.write(gandalf.serialize(partition, 16));
				byte[] bb = bun.serialize();
				baos.write(gandalf.getVarInt(bb.length));
				baos.write(bb);
			} catch (IOException e) {
				log.log(Level.SEVERE, "ERROR creating Topic", e);
				System.exit(1);
			}
			topic = baos.toByteArray();
		}
		
		public byte[] serialize() {
			return topic;
		}

		private byte topic[];
	}

	private class Bundle {
		Bundle() {
			messages = new ArrayList<TankMessage>();
		}

		Bundle(ArrayList<byte[]> msgs) {
			messages = new ArrayList<TankMessage>();
			for (byte[] m : msgs) addMsg(new TankMessage(0l, m));
		}

		void addMsg(TankMessage tm) {
			messages.add(tm);
		}

		public byte[] serialize() {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
                        try {
				byte flags = 0;
				if (messages.size() <= 15) {
					flags |= (messages.size() << 2);
				}
				baos.write(gandalf.serialize(flags, 8));

				if (messages.size() > 15) {
					baos.write(gandalf.getVarInt(messages.size()));
				}

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

	private ByteManipulator gandalf;
        private String tankHost;
        private int tankPort;
	private int reqSeqNum;
	private Socket client;
	private BufferedInputStream bis;
	private OutputStream socketOutputStream;
	private Logger log;
	private int clientReqID;
	private ArrayList<TankMessage> messages;

	public static final byte HaveKey = 1;
	public static final byte UseLastSpecifiedTS = 2;
	public static final long RETRY_INTERVAL = 50;

	private short reqType;
	public static final short PUBLISH_REQ = 1;
	public static final short CONSUME_REQ = 2;
}
