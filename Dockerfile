FROM ubuntu:xenial
RUN apt -y update
RUN apt -y install openjdk-9-jdk-headless
ADD src /src/
WORKDIR /src
RUN javac *.java
ENTRYPOINT ["/usr/bin/java", "TestApp"];
