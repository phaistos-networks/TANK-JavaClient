# TANK-JavaClient
Java Client for [TANK](https://github.com/phaistos-networks/TANK).  

## Status ##
This is a work in progress.  
Currently only single topic/partition Publish and Consume operations are supported.  

## Get ##
```bash
git clone https://github.com/phaistos-networks/TANK-JavaClient.git
make TankClient
```

## Usage ##
### import ###
```java
import gr.phaistosnetworks.TANK.*;
```

### Publish ###
```java
TankClient tc = new TankClient(host, port);
ArrayList<TankMessage> data = new ArrayList<TankMessage>();
data.add(new TankMessage(new String("Hello World").getBytes()));
tc.publish(topic, partition, data);
```

### Consume ###
```java
TankClient tc = new TankClient(host, port);
ArrayList<TankMessage> data = tc.consume(topic, partition, sequenceNum);
for (TankMessage tm : data) System.out.println(new String(tm.getMessage()));

```

## API ##
https://phaistos-networks.github.io/TANK-JavaClient/

## License ##
apache v2.0
