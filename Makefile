all: check

build:
	rebar compile

check: build
	rebar eunit

clean:
	rebar clean
