REBAR?=$(shell echo `pwd`/bin/rebar)

all: check

build:
	./configure
	$(REBAR) compile

check: build
	$(REBAR) eunit

clean:
	$(REBAR) clean
