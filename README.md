# TANK-JavaClient
Java Client for [TANK](https://github.com/phaistos-networks/TANK).  

## Status ##
This is a work in progress.  
Currently only single topic/partition Publish and Consume operations are (mostly) supported.  
DO NOT USE for anything other than testing, since everything is subject to change.  

## Get ##
```bash
git clone https://github.com/phaistos-networks/TANK-JavaClient.git
make TankClient
```

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
  key.getBytes(),
  message.getBytes()));

tc.publish(pubReq);
```

### Publish Response ###
```java
List<TankResponse> response = tc.publish(pubReq);
for (TankResponse tr : response) {
  if (tr.hasError()) {
    if (tr.getError() == TankClient.ERROR_NO_SUCH_TOPIC) {
      System.out.println("Error, topic " + tr.getTopic() + " does not exist !");
    } else if (tr.getError() == TankClient.ERROR_NO_SUCH_PARTITION) {
      System.out.println("Error, topic " + tr.getTopic() + " doe not have a partition " + tr.getPartition());
    }
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
        "seq: " + tm.getSeqId()
        + " ts: " + tm.getTimestamp()
        + (tm.haveKey()) ? " key: " + new String(tm.getKey()) : ""
        + " message: " + new String(tm.getMessage()));
  }
}
```

### Consume Next ###
```java
while (true) {
  responses = tc.consume(consume);
  consume = new TankRequest(TankClient.CONSUME_REQ);
  for (TankResponse tr : responses) {

    if (tr.getFetchSize() > fetchSize) {
      fetchSize = tr.getFetchSize();
    }

    consume.consumeTopicPartition(
        tr.getTopic(),
        tr.getPartition(),
        tr.getNextSeqId(),
        fetchSize);
  }
}

```

## API ##
https://phaistos-networks.github.io/TANK-JavaClient/

## License ##
Apache v2.0
