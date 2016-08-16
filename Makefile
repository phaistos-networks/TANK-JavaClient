JAVAC= /usr/bin/javac
CURDIR= $(shell pwd)
SRC= $(CURDIR)/src
DST= gr/phaistosnetworks/TANK
CPATH= $(CURDIR):$(CURDIR)/ext/snappy-java-1.1.2.6.jar
FLAGS= -Xdiags:verbose -Xlint:unchecked -Xlint:deprecation -cp $(CPATH)

all: 
	mkdir -p $(DST)
	cd $(SRC) && $(JAVAC) $(FLAGS) *.java
	mv $(SRC)/*.class $(DST)
	cd $(CURDIR)
	$(JAVAC) $(FLAGS) TestApp.java

clean:
	find . -type f -name '*.class' | xargs rm -f

