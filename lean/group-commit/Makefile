LAKE ?= lake
EXPLORE = .lake/build/bin/explore

all: check

check:
	$(LAKE) build

explore:
	$(LAKE) build explore
	$(EXPLORE) original n=2 toggle
	$(EXPLORE) fixed n=2 toggle inv
	$(EXPLORE) fixed n=3 inv max=20000000

clean:
	rm -rf .lake

.PHONY: all check explore clean
