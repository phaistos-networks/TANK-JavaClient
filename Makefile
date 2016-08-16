JAVAC= /usr/bin/javac
JAVADOC= /usr/bin/javadoc
CURDIR= $(shell pwd)
SRC= $(CURDIR)/src
DOC= $(CURDIR)/doc
DST= gr/phaistosnetworks/TANK
PACKAGE= gr.phaistosnetworks.TANK
CPATH= $(CURDIR):$(CURDIR)/ext/snappy-java-1.1.2.6.jar
FLAGS= -Xdiags:verbose -Xlint:unchecked -Xlint:deprecation -cp $(CPATH)

all:	TankClient testApp doc

doc: 	$(SRC)/*.java
	mkdir -p $(DOC)
	$(JAVADOC) -d $(DOC) -sourcepath $(SRC) -subpackages gr

clean:
	find . -type f -name '*.class' | xargs rm -f

testApp: TestApp.java
	$(JAVAC) $(FLAGS) TestApp.java

TankClient: $(SRC)/*.java
	mkdir -p $(DST)
	cd $(SRC) && $(JAVAC) $(FLAGS) *.java
	mv $(SRC)/*.class $(DST)

