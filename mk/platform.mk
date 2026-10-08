# The platform the build is for (included by the Makefile): PLATFORM, from the machine unless the command line says otherwise.
#   macos | ios | ios-sim | mac-catalyst | android | linux | linux-musl | windows
HOSTOS := $(shell uname -s | tr '[:upper:]' '[:lower:]')
ifeq ($(OS),Windows_NT)
HOSTOS := windows
endif
ifneq (,$(findstring mingw,$(HOSTOS))$(findstring msys,$(HOSTOS)))
HOSTOS := windows
endif
ifndef PLATFORM
ifeq ($(HOSTOS),darwin)
PLATFORM := macos
else ifeq ($(HOSTOS),windows)
PLATFORM := windows
else
PLATFORM := linux
endif
endif

EXE :=
ifeq ($(PLATFORM),windows)
EXE := .exe
endif
