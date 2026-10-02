# Makefile for MultiprocessWal TLA+ Model Checking

TLA2TOOLS ?= tla2tools.jar
JAVA_OPTS ?= -XX:+UseParallelGC -Xmx4g
TLC_OPTS ?= -workers auto -cleanup

SPEC = MultiprocessWal.tla
CFG ?= MultiprocessWal.cfg

all: check MultiprocessWal.pdf

check: $(TLA2TOOLS)
	java $(JAVA_OPTS) -cp $(TLA2TOOLS) tlc2.TLC $(TLC_OPTS) -config $(CFG) $(SPEC)

MultiprocessWal.pdf: $(TLA2TOOLS) $(SPEC)
	java -cp $(TLA2TOOLS) tla2tex.TLA -shade -latexCommand pdflatex $(SPEC)
	pdflatex MultiprocessWal.tex
	rm -f MultiprocessWal.tex MultiprocessWal.aux MultiprocessWal.log

$(TLA2TOOLS):
	curl -LO https://github.com/tlaplus/tlaplus/releases/download/v1.8.0/tla2tools.jar

clean:
	rm -rf states/ MultiprocessWal.pdf

.PHONY: all check clean
