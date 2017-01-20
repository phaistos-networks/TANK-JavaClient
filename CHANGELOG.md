## 0.5 "Ord Mantell" (2017-01-19)

Features:

 - Performance improvement release.

## 0.3 "Coruscant" (2016-11-17)

Features:

 - Bug fixes release.

API Changes:
 - Key and Message are now ByteBuffers.
 - Convenience methods available for string and byte arrays

## 0.2 "Carrick Station" (2016-09-15)

Features:

  - Implemented snappy compression for message data > 1kB.
  - Added highWaterMark and firstAvailSeqNum to responses. Check for tankResponse.getError() == TankClient.ERROR_OUT_OF_BOUNDS.

API Changes:
  - All seqIds have been renamed to seqNums.


## 0.1 "Tython" (2016-08-30)

  - Initial release

Features:

  - Create publish requests with multiple topics / partitions.
  - Create consume requests with mulciple topics / partitions.

Known Issues / Limitations / Bugs:

  - Snappy compression not implemented.
  - For snappy decompression, you need [org.xerial.snappy.Snappy](https://github.com/xerial/snappy-java).

