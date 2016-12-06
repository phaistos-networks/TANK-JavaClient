JAVAC= /usr/bin/javac
JAVADOC= /usr/bin/javadoc
CURDIR= $(shell pwd)
SRC= $(CURDIR)/src
VER= 0.4.0
DOC= $(CURDIR)/doc/$(VER)
DST= gr/phaistosnetworks/tank
PACKAGE= gr.phaistosnetworks.tank
CPATH= $(CURDIR):$(CURDIR)/ext/*
FLAGS= -Xdiags:verbose -Xlint:unchecked -Xlint:deprecation -cp $(CPATH)

all:	client testapp

doc: 	
	mkdir -p $(DOC)
	$(JAVADOC) -cp $(CPATH) -d $(DOC) -sourcepath $(SRC) $(PACKAGE)

clean:
	find . -type f -name '*.class' | xargs rm -f

testapp: TestApp.java
	$(JAVAC) $(FLAGS) TestApp.java Tool.java

teststyle:
	java -jar $(CURDIR)/ext/* com.puppycrawl.tools.checkstyle.Main -c $(CURDIR)/ext/google_checks.xml TestApp.java Tool.java

client: 
	mkdir -p $(DST)
	$(JAVAC) -d . $(FLAGS) $(SRC)/$(DST)/*.java

style:
	java -jar $(CURDIR)/ext/* com.puppycrawl.tools.checkstyle.Main -c $(CURDIR)/ext/google_checks.xml $(SRC)/$(DST)/*.java

jar:
	jar cvf $(DOC)/tank-$(VER).jar gr/

.PHONY: doc
