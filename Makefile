REBAR := $(shell which rebar3 2>/dev/null || which ./rebar3)
SUBMODULES = build_utils
SUBTARGETS = $(patsubst %,%/.git,$(SUBMODULES))

UTILS_PATH := build_utils
TEMPLATES_PATH := .

# Name of the service
SERVICE_NAME := gunner
# Service image default tag
SERVICE_IMAGE_TAG ?= $(shell git rev-parse HEAD)

BUILD_IMAGE_NAME := build-erlang
BUILD_IMAGE_TAG := 19ff48ccbe09b00b79303fc6e5c63a3a9f8fd859

CALL_ANYWHERE := \
	submodules \
	all compile xref lint check_format format dialyze cover release clean distclean

CALL_W_CONTAINER := $(CALL_ANYWHERE) test

.PHONY: $(CALL_W_CONTAINER) all

all: compile

-include $(UTILS_PATH)/make_lib/utils_container.mk
-include $(UTILS_PATH)/make_lib/utils_image.mk

$(SUBTARGETS): %/.git: %
	git submodule update --init $<
	touch $@

submodules: $(SUBTARGETS)

compile: submodules
	$(REBAR) compile

xref:
	$(REBAR) xref

lint:
	$(REBAR) lint

check_format:
	$(REBAR) fmt -c

format:
	$(REBAR) fmt -w

dialyze:
	$(REBAR) dialyzer

release: submodules
	$(REBAR) as prod release

clean:
	$(REBAR) cover -r
	$(REBAR) clean

distclean:
	$(REBAR) clean
	rm -rf _build

cover:
	$(REBAR) cover

# CALL_W_CONTAINER
test: submodules
	$(REBAR) do eunit, ct
