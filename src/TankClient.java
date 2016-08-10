import java.net.*;
import java.io.*;
import java.nio.*;
import java.util.ArrayList;
import java.util.logging.*;

class TankClient {
        public TankClient(String tHost, int tPort, String tTopic, int tPartition) {
                tankHost = tHost;
                tankPort = tPort;
                tankTopic = tTopic;
                tankPartition = tPartition;
		log = Logger.getLogger("tankClient");
		gandalf = new ByteManipulator();

                try {
			while (true) {
				try {
					client = new Socket(tankHost, tankPort);
				} catch (Exception e) {
					log.log(Level.SEVERE, "ERROR opening socket", e);
					Thread.sleep(100);
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
			if ( !getPing(bis) ) {
				log.severe("ERROR: No Ping Received");
				System.exit(1);
			} else {
				log.fine("PING OK");
			}
		} catch (Exception e) {
			log.log(Level.SEVERE, "ERROR opening Streams", e);
			System.exit(1);
		}
        }

        public ArrayList<TankMessage> get(long rSeqNum) {
		log.fine("Received get "+rSeqNum);
		ArrayList<TankMessage> messages = new ArrayList<TankMessage>();

		FetchTopic topics[] = new FetchTopic[1];
		topics[0] = new FetchTopic(tankTopic, tankPartition, rSeqNum, 20000l);

		byte req[] = fetchReq(0l, 0l, "java", 1000l, 0l, topics);
		byte rsize[] = (gandalf.serialize(req.length-5, 32));
		for (int i=0; i<4; i++) {
			req[i+1] = rsize[i];
		}

		try {
			socketOutputStream.write(req);
			ByteManipulator input = new ByteManipulator();
			boolean incomplete = false;
			while (true) {
				int av = bis.available();
				log.fine("available: "+av);
				if (av == 0) {
					Thread.sleep(10);
					continue;
				}

				byte ba[] = new byte[av];
				bis.read(ba, 0, av);

				if (incomplete)
					input.append(ba);
				else
					input = new ByteManipulator(ba);

				byte resp = (byte)input.deSerialize(8);
				long payloadSize = input.deSerialize(32);
				if (resp != 2) {
					log.severe("Did not receive expected response type 2 (" +resp+ ")");
					System.exit(1);
				}

				if (payloadSize > input.getRemainingLength()) {
					log.warning("Received packet incomplete ");
					incomplete = true;
					input.resetOffset();
					continue;
				} else
					incomplete = false;

				log.fine("resp: " + resp);
				log.fine("payload size: " + payloadSize);

				messages =  getMessages(input);

				for (Handler h : log.getHandlers())
					h.flush();

				return messages;
			}
		} catch (Exception e) {
			log.log(Level.SEVERE, "ERROR getting message", e);
			e.printStackTrace();
			System.exit(1);
		}
		return null;
	}

	private byte[] fetchReq(long clientVersion, long reqID, String clientId, long maxWait, long minBytes, FetchTopic[] topics) { 
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
			for (int i=0; i< topics.length; i++)
				baos.write(topics[i].get());
			
		} catch (Exception e) {
			log.log(Level.SEVERE, "ERROR creating fetch request", e);
			System.exit(1);
		}
		return baos.toByteArray();
	}


	private ArrayList<TankMessage> getMessages(ByteManipulator input) {
		ArrayList<Chunk> chunkList = new ArrayList<Chunk>();
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

			long partitionID = input.deSerialize(16);
			if (partitionID == 65535) {
				log.warning("Topic Not Found ");
				return null;
			} else {
				// Partitions
				for (int p=0 ; p < totalPartitions; p++) {
					if (p != 0)
						partitionID = input.deSerialize(16);

					byte errorOrFlags = (byte)input.deSerialize(8);
					log.fine("Partition ID: " + partitionID);
					log.fine(String.format("ErrorOrFlags : %x\n", errorOrFlags));

					if ((errorOrFlags & 0xFF) == 0xFF) {
						log.warning("Unknown Partition\n ABORT ABORT");
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
						log.warning("FirstAvailSeqNum : " + firstAvailSeqNum);
						log.warning("HighWatermark : " + highWaterMark);
						log.warning("Out of bounds, ABORT ABORT !!");
						return null;
					}
					chunkList.add(new Chunk(topic, partitionID, errorOrFlags, baseAbsSeqNum, highWaterMark, chunkLength));
				}
			}
		}

		//Chunks
		ArrayList<TankMessage> messages = new ArrayList<TankMessage>();
		long curSeqNum = 0;
		for (Chunk c : chunkList) {
			while (input.getRemainingLength() > 0) {
				log.fine("Remaining Length: "+input.getRemainingLength());
				long bundleLength = input.getVarInt();
				log.finer("Bundle length : " +bundleLength);
				if (bundleLength > input.getRemainingLength()) {
					log.fine("Bundle Incomplete (remaining bytes: "+input.getRemainingLength()+")");
					return messages;
				}
				input.flushOffset();

				byte flags = (byte)input.deSerialize(8);
				long messageCount = (flags >> 2) &0xf;
				long compressed = flags &0x3;
				long sparse = (flags >> 6) &0xf;
				log.finer("Bundle compressed : " +compressed);
				log.finer("Bundle SPARSE : " +sparse);

				if (messageCount == 0)
					messageCount = input.getVarInt();
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
						return null;
					}
				} else {
					chunkMsgs = input;
				}

				long lastTimestamp = 0l;
				long prevSeqNum = firstMessageNum;
				for (int i = 0; i < messageCount; i++) {
					if (curSeqNum == 0)
						curSeqNum = c.baseAbsSeqNum-1;
					log.finer("#### Message "+ (i+1) +" out of "+messageCount);
					flags = (byte)chunkMsgs.deSerialize(8);
					log.finer(String.format("flags : %d", flags));
					if (sparse == 1) {
						if (i == 0)
							log.finer("seq num: " + firstMessageNum);
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
						lastTimestamp = chunkMsgs.deSerialize(64);
						log.finer("TimeStamp : " + lastTimestamp);
					}

					if ((flags & HaveKey) == 1) {
						String key = chunkMsgs.getStr8();
						log.finer("We have a key and it is : " + key);
					}

					long contentLength = chunkMsgs.getVarInt();
					log.finer("Content Length: " + contentLength);

					byte message[] = chunkMsgs.get((int)contentLength);
					log.finest(new String(message));
					messages.add(new TankMessage(c.topic, c.partitionID, curSeqNum, 0l, message));
				}
			}
		}
		return messages;
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
		public Chunk(String t, long pid, byte eof, long basn, long hwm, long l) {
			topic = t;
			partitionID = pid;
			errorOrFlags = eof;
			baseAbsSeqNum = basn;
			highWaterMark = hwm;
			length = l;
		}
		String topic;
		long partitionID;
		byte errorOrFlags;
		long highWaterMark;
		long length;
		long baseAbsSeqNum;
	}

	private class FetchTopic {
		FetchTopic(String name, long partitionID, long seqNum, long fetchSize) {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			try {
				baos.write(gandalf.getStr8(name));
				baos.write(gandalf.serialize(1l, 8));
				baos.write(gandalf.serialize(partitionID, 16));
				baos.write(gandalf.serialize(seqNum, 64));
				baos.write(gandalf.serialize(fetchSize, 32));
			} catch (IOException e) {
				log.log(Level.SEVERE, "ERROR creating FetchTopic", e);
				System.exit(1);
			}
			topic = baos.toByteArray();
		}
		
		public byte[] get() {
			return topic;
		}

		private byte topic[];
	}

	private ByteManipulator gandalf;
        private String tankHost;
        private int tankPort;
        private String tankTopic;
        private int tankPartition;
	private int reqSeqNum;
	private Socket client;
	private BufferedInputStream bis;
	private OutputStream socketOutputStream;
	private Logger log;

	public static final byte HaveKey = 1;
	public static final byte UseLastSpecifiedTS = 2;
}
