# TANK-JavaClient
Java Client for [TANK](https://github.com/phaistos-networks/TANK).  

## Status ##
This is a work in progress.  
DO NOT USE for anything other than testing, since everything is subject to change.  

## Get ##
```bash
git clone https://github.com/phaistos-networks/TANK-JavaClient.git
make TankClient
```

## Downloads / Doc ##
[Releases](https://github.com/phaistos-networks/TANK-JavaClient/releases)

## Requirements ##
 - Java (tested on 8+)
 - [org.xerial.snappy.Snappy](https://github.com/xerial/snappy-java)

## Usage ##
### import ###
```java
import gr.phaistosnetworks.tank.*;
```

### Publish ###
```java
TankClient tc = new TankClient(host, port);
TankRequest pubReq = new TankRequest(TankClient.PUBLISH_REQ);
pubRec.publishMessage(
  topic,
  partition,
  new TankMessage(
    key,
    message));

tc.publish(pubReq);
```

### Publish Response ###
```java
List<TankResponse> response = tc.publish(pubReq);
for (TankResponse tr : response) {
  if (tr.hasError()) {
    System.out.println("Error, for topic " + tr.getTopic() + ":" + tr.getPartition());
  }
}
```

### Consume ###
```java
TankClient tc = new TankClient(host, port);
TankRequest consumeReq = new TankRequest(TankClient.CONSUME_REQ);
consumeReq.consumeTopicPartition(topic, partition, id, fetchSize);
List<TankResponse> responses = tc.consume(consumeReq);

for (TankResponse tr : responses) {
  System.out.println("topic: " + tr.getTopic() + " partition: " + tr.getPartition());
  for (TankMessage tm : tr.getMessages()) {
    System.out.println(
        "seq: " + tm.getSeqNum()
        + " ts: " + tm.getTimestamp()
        + ((tm.haveKey()) ? " key: " + tm.getKeyAsString() : "")
        + " message: " + tm.getMessageAsString());
  }
}
```

### Consume Next ###
```java
long nextSeqNum = 0L;
while (true) {
  responses = tc.consume(consumeReq);
  consumeReq = new TankRequest(TankClient.CONSUME_REQ);
  for (TankResponse tr : responses) {

    // Detect if fetchSize is too small, and increase it
    if (tr.getFetchSize() > fetchSize) {
      fetchSize = tr.getFetchSize();
    }

    // Detect if requested Seq Num is out of bounds and handle it
    if (tr.hasError() && tr.getError() == TankClient.ERROR_OUT_OF_BOUNDS) {
      if (tr.getRequestSeqNum() < tr.getFirstAvailSeqNum()) {
        nextSeqNum = tr.getFirstAvailSeqNum();
      } else if (tr.getRequestSeqNum() > tr.getHighWaterMark()) {
        nextSeqNum = tr.getHighWaterMark();
      }
    } else {
      nextSeqNum = tr.getNextSeqNum();
    }
    consumeReq.consumeTopicPartition(
      tr.getTopic(),
      tr.getPartition(),
      nextSeqNum,
      fetchSize);
  }
}

```

## License ##
Apache v2.0
