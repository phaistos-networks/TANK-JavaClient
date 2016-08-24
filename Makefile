JAVAC= /usr/bin/javac
JAVADOC= /usr/bin/javadoc
CURDIR= $(shell pwd)
SRC= $(CURDIR)/src
DOC= $(CURDIR)/doc
DST= gr/phaistosnetworks/TANK
PACKAGE= gr.phaistosnetworks.TANK
CPATH= $(CURDIR):$(CURDIR)/ext/snappy-java-1.1.2.6.jar
FLAGS= -Xdiags:verbose -Xlint:unchecked -Xlint:deprecation -cp $(CPATH)

all:	client testapp

doc: 	
	mkdir -p $(DOC)
	$(JAVADOC) -cp $(CPATH) -d $(DOC) -sourcepath $(SRC) $(PACKAGE)

clean:
	find . -type f -name '*.class' | xargs rm -f

testapp: TestApp.java
	$(JAVAC) $(FLAGS) TestApp.java

client: 
	mkdir -p $(DST)
	$(JAVAC) -d . $(FLAGS) $(SRC)/$(DST)/*.java

style:
	java -jar $(CURDIR)/ext/checkstyle-7.1-all.jar com.puppycrawl.tools.checkstyle.Main -c $(CURDIR)/ext/google_checks.xml $(SRC)/$(DST)/*.java

.PHONY: doc
