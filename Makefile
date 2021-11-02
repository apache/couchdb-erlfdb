all: check

build:
	@rebar3 compile

check: build
	@rebar3 eunit

clean:
	@rebar3 clean
	@rm -rf _build
