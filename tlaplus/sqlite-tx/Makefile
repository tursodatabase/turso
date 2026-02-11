# Makefile for SqliteTx TLA+ Model Checking

TLA2TOOLS ?= tla2tools.jar
JAVA_OPTS ?= -XX:+UseParallelGC -Xmx4g
TLC_OPTS ?= -workers auto -cleanup

SPEC = SqliteTx.tla
CFG = SqliteTx.cfg

.PHONY: check clean

check: $(TLA2TOOLS)
	java $(JAVA_OPTS) -cp $(TLA2TOOLS) tlc2.TLC $(TLC_OPTS) -config $(CFG) $(SPEC)

SqliteTx.pdf: $(TLA2TOOLS) $(SPEC)
	java -cp $(TLA2TOOLS) tla2tex.TLA -shade $(SPEC)
	pdflatex SqliteTx.tex
	pdflatex SqliteTx.tex
	rm -f SqliteTx.tex SqliteTx.aux SqliteTx.log

$(TLA2TOOLS):
	curl -LO https://github.com/tlaplus/tlaplus/releases/download/v1.8.0/tla2tools.jar

clean:
	rm -rf states/ SqliteTx.pdf
