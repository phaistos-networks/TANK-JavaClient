class TankMessage {

	public TankMessage(String t, long p, long sid, long ts, byte[] m) {
		topic = t;
		partition = p;
		seqID = sid;
		timestamp = ts;
		message = m;
	}

	public String getTopic() { return topic; }
	public long getPartition() { return partition; }
	public long getSeqID() { return seqID; }
	public long getTimestamp() { return timestamp; }
	public byte[] getMessage() { return message; }

	private String topic;
	private long partition;
	private long seqID;
	private long timestamp;
	private byte[] message;
}
