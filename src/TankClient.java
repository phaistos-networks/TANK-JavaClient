import java.net.*;
import java.io.*;
import java.nio.*;

class TankClient implements Runnable {
        public TankClient(String tHost, int tPort, String tTopic, int tPartition, int rSeqNum) {
                tankHost = tHost;
                tankPort = tPort;
                tankTopic = tTopic;
                tankPartition = tPartition;
		reqSeqNum = rSeqNum;
        }

        public void run() {
                try {
                        clientSocket = new DatagramSocket();
                        Socket client;
			while (true) {
				try {
					client = new Socket(tankHost, tankPort);
				} catch (Exception e) {
					e.printStackTrace();
					Thread.sleep(100);
					continue;
				}
				//client.setSoTimeout(1000);
				client.setTcpNoDelay(true);
				client.setKeepAlive(true);
				client.setReuseAddress(true);
				System.out.println("Connected to "+ client.getRemoteSocketAddress()
						+ "\n + recv buffer size: "+ client.getReceiveBufferSize()
						+ "\n + send buffer size: "+ client.getSendBufferSize()
						+ "\n + timeout: " + client.getSoTimeout()
						+ "\n + soLinger: " + client.getSoLinger()
						+ "\n + nodelay: " + client.getTcpNoDelay()
						+ "\n + keepalive: " + client.getKeepAlive()
						+ "\n + oobinine: " + client.getOOBInline()
						+ "\n + reuseAddress: " + client.getReuseAddress());

				BufferedInputStream bis = new BufferedInputStream(client.getInputStream());
				gandalf = new ByteManipulator();
				byte[] ba;

				if ( !getPing(bis) ) {
					System.err.println("ERROR: No Ping Received");
					System.exit(1);
				} else {
					System.out.println(" + PING OK");
				}

				FetchTopic topics[] = new FetchTopic[1];

				//FetchTopic(String name, long partitionID, long seqNum, long fetchSize)
				topics[0] = new FetchTopic(tankTopic, tankPartition, reqSeqNum, 1400l);
				//topics[1] = new FetchTopic("foo", 0l, 0l, 1400l);

				byte req[] = fetchReq(0l, 0l, "java", 0l, 0l, topics);
				byte rsize[] = (gandalf.serialize(req.length-5, 32));
				for (int i=0; i<4; i++) {
					req[i+1] = rsize[i];
				}

				OutputStream socketOutputStream = client.getOutputStream();
				socketOutputStream.write(req);

				getMessage(bis);
                                client.close();
				break;
                        }
                } catch (Exception e) {
			e.printStackTrace();
                        System.exit(0);
                }
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
			e.printStackTrace();
			System.exit(1);
		}
		return baos.toByteArray();
	}

	private void getMessage(BufferedInputStream bis) {
		try {
			while (true) {
				int av = bis.available();
				if (av == 0) {
					Thread.sleep(10);
					continue;
				}
				System.out.println(" ++ available: "+av);

				byte ba[] = new byte[av];
				bis.read(ba, 0, av);
				printMessage(ba);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void printMessage(byte[] ba) {
		ByteManipulator input = new ByteManipulator(ba);
		System.out.println(" ++ resp: " + input.deSerialize(8));
		System.out.println(" ++ payload size: " + input.deSerialize(32));
		System.out.println(" ++ header size: " + input.deSerialize(32));
		System.out.println(" ++ reqid: "+ input.deSerialize(32));
		System.out.format (" ++ topics count: %d\n", input.deSerialize(8));
		System.out.println(" ++ topic name: " + input.getStr8());
		System.out.println(" +++ Total Partitions: "+ input.deSerialize(8));

		long partitionID = input.deSerialize(16);
		if (partitionID == 255) {
			System.out.println(" +++ Topic Not Found ");
			return;
		} else {
			System.out.println(" ++++ Partition ID: " + partitionID);
			byte error = (byte)input.deSerialize(8);
			System.out.println(" ++++ Error: " + error);
			if (error == 0xff) {
				System.out.println(" +++++ Unknow Partition");
				return;
			}
			System.out.println(" ++++ First Seq # : " +input.deSerialize(64));
			System.out.println(" ++++ High Watermark : " +input.deSerialize(64));
			System.out.println(" ++++ Chunk Length : " +input.deSerialize(32));
			long bundleLength = input.getVarInt();
			while (bundleLength <= input.getRemainingLength()) {
				System.out.println(" +++++ Bundle length : " +bundleLength);
				System.out.println(" +++++ Remaining : " +input.getRemainingLength());
				long flags = input.deSerialize(8);
				long messageCount = (flags >> 2) &0xf;
				long compressed = flags &0x3;
				long sparse = (flags >> 6) &0xf;
				System.out.println(" +++++ Bundle compressed : " +compressed);
				System.out.println(" +++++ Bundle SPARSE : " +sparse);
				if (messageCount == 0)
					messageCount = input.getVarInt();
				System.out.println(" +++++ Messages in set : " +messageCount);
				long firstMessageNum = 0l;
				long lastMessageNum = 0l;
				if (sparse == 1) {
					firstMessageNum = input.deSerialize(64);
					System.out.println(" +++++ First message: "+ firstMessageNum);
					if (messageCount > 1) {
						lastMessageNum = input.getVarInt() + 1 + firstMessageNum;
						System.out.println(" +++++ Last message: " + lastMessageNum);
					}
				}
				long lastTimestamp = 0l;
				long prevSeqNum = firstMessageNum;
				long curSeqNum = 0;
				for (int i = 0; i < messageCount; i++) {
					flags = input.deSerialize(8);
					if (sparse == 1) {
						if (i == 0)
							System.out.println(" ++++++ seq num: " + firstMessageNum);
						else if (i!=0 && i!=messageCount-1) {
							curSeqNum = (input.getVarInt() + 1 + prevSeqNum);
							System.out.println(" ++++++ seq num: " + curSeqNum);
							prevSeqNum = curSeqNum;
						} else
							System.out.println(" ++++++ seq num: " + lastMessageNum);
					}
					if ((flags & UseLastSpecifiedTS) == 0) 
						lastTimestamp = input.deSerialize(64);

					if ((flags & HaveKey) == 1) {
						System.out.println(" ++++++ We have a key and it is : " + input.getStr8());
					}
					long contentLength = input.getVarInt();
					System.out.println(" ++++++ Content Length: " + contentLength);
					byte data[] = data = input.get((int)contentLength);
					for (byte b : data)
						System.out.format("%c", b, b);
					System.out.println();
				}
				if (input.getRemainingLength() > 0)
					bundleLength = input.getVarInt();
			}
		}
	}


	private boolean getPing(BufferedInputStream bis) {
		try {
			int av = bis.available();
			if (av == 0) return false;

			byte b = (byte)bis.read();
			if (b != 0x3) return false;
			bis.skip(4);
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
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
			} catch (IOException ioe) {
				ioe.printStackTrace();
				System.exit(1);
			}
			topic = baos.toByteArray();
		}
		
		public byte[] get() {
			return topic;
		}

		byte topic[];
	}

	private ByteManipulator gandalf;
        private String tankHost;
        private int tankPort;
        private String tankTopic;
        private int tankPartition;
	private int reqSeqNum;
        private DatagramSocket clientSocket;

	public static final byte HaveKey = 1;
	public static final byte UseLastSpecifiedTS = 2;
}
