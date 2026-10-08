# The loadable extension, for every platform (included by the Makefile).
#
#   make extension                         for the machine it runs on  -> dist/multiwriter.so | .dylib | .dll
#   make extension PLATFORM=macos [ARCH=x86_64|arm64]       (no ARCH: one universal binary)
#   make extension PLATFORM=ios | ios-sim | mac-catalyst    (macOS with Xcode)
#   make extension PLATFORM=android ARCH=arm64-v8a|armeabi-v7a|x86_64|x86 ANDROID_NDK=/path/to/ndk
#   make extension PLATFORM=linux | linux-musl | windows     (on that system; windows: MSYS2 mingw64)
#   make xcframework | aar | package      the Apple XCFramework, the Android AAR, the archive of the platform
#   make test-loadable                    loads the extension into a stock SQLite and runs concurrent writers (host platforms and Android x86_64)
#   make version                          the version (MW_VERSION in src/multiwriter.h)
#
# The extension is the engine alone, compiled with MW_LOADABLE: it calls SQLite through the table of routines that the host passes to sqlite3_multiwriter_init (sqlite3ext.h), so it needs nothing
# from the SQLite it is loaded into except its version (3.14 or later; the engine is tested with the SQLite of third_party/sqlite).

VERSION := $(shell sed -n 's/^\#define MW_VERSION[[:space:]]*"\([^"]*\)".*/\1/p' src/multiwriter.h)
.PHONY: version
version:
	@echo $(VERSION)

EXT_BASE := multiwriter
EXT_CFLAGS := $(CFLAGS) -DMW_LOADABLE -fPIC -fvisibility=hidden
EXT_LDFLAGS :=
EXT_SUFFIX := so
EXT_STRIP = strip --strip-unneeded $@
# where the extension of this platform and architecture is built (a separate directory for each: the objects of one are not those of another)
EXT_ARCHDIR := $(PLATFORM)$(if $(ARCH),-$(ARCH))
EXT_BUILD := $(BUILD)/ext/$(EXT_ARCHDIR)

ifeq ($(PLATFORM),macos)
EXT_SUFFIX := dylib
ifdef ARCH
EXT_ARCHFLAGS := -arch $(ARCH)
else
EXT_ARCHFLAGS := -arch x86_64 -arch arm64
endif
EXT_CFLAGS += $(EXT_ARCHFLAGS) -mmacosx-version-min=11.0
EXT_LDFLAGS += $(EXT_ARCHFLAGS) -dynamiclib -mmacosx-version-min=11.0 -headerpad_max_install_names
EXT_STRIP = strip -x -S $@
else ifeq ($(PLATFORM),ios)
EXT_SUFFIX := dylib
EXT_SDK := -isysroot $(shell xcrun --sdk iphoneos --show-sdk-path 2>/dev/null) -miphoneos-version-min=11.0
EXT_CFLAGS += -arch arm64 $(EXT_SDK)
EXT_LDFLAGS += -arch arm64 $(EXT_SDK) -dynamiclib -headerpad_max_install_names
EXT_STRIP = strip -x -S $@
else ifeq ($(PLATFORM),ios-sim)
EXT_SUFFIX := dylib
EXT_SDK := -isysroot $(shell xcrun --sdk iphonesimulator --show-sdk-path 2>/dev/null) -mios-simulator-version-min=11.0
EXT_CFLAGS += -arch x86_64 -arch arm64 $(EXT_SDK)
EXT_LDFLAGS += -arch x86_64 -arch arm64 $(EXT_SDK) -dynamiclib -headerpad_max_install_names
EXT_STRIP = strip -x -S $@
else ifeq ($(PLATFORM),mac-catalyst)
EXT_SUFFIX := dylib
MAC_CATALYST_DEPLOYMENT_TARGET ?= 14.0
ifdef ARCH
EXT_SDK := -isysroot $(shell xcrun --sdk macosx --show-sdk-path 2>/dev/null) -target $(ARCH)-apple-ios$(MAC_CATALYST_DEPLOYMENT_TARGET)-macabi
EXT_CFLAGS += $(EXT_SDK)
EXT_LDFLAGS += $(EXT_SDK) -dynamiclib -headerpad_max_install_names
else
# no ARCH: one build for each, put together with lipo
EXT_UNIVERSAL := true
endif
EXT_STRIP = strip -x -S $@
else ifeq ($(PLATFORM),android)
ifndef ARCH
$(error Android needs ARCH=arm64-v8a, armeabi-v7a, x86_64 or x86)
endif
ifndef ANDROID_NDK
$(error Android needs ANDROID_NDK, the path of the NDK)
endif
ANDROID_API ?= 26
NDK_HOST := $(if $(filter darwin,$(HOSTOS)),darwin-x86_64,linux-x86_64)
NDK_BIN := $(ANDROID_NDK)/toolchains/llvm/prebuilt/$(NDK_HOST)/bin
ifeq ($(ARCH),arm64-v8a)
ANDROID_TRIPLE := aarch64-linux-android$(ANDROID_API)
else ifeq ($(ARCH),armeabi-v7a)
ANDROID_TRIPLE := armv7a-linux-androideabi$(ANDROID_API)
else ifeq ($(ARCH),x86_64)
ANDROID_TRIPLE := x86_64-linux-android$(ANDROID_API)
else ifeq ($(ARCH),x86)
ANDROID_TRIPLE := i686-linux-android$(ANDROID_API)
else
$(error Android ARCH must be arm64-v8a, armeabi-v7a, x86_64 or x86)
endif
CC := $(NDK_BIN)/$(ANDROID_TRIPLE)-clang
EXT_CFLAGS += -D_GNU_SOURCE
CFLAGS += -D_GNU_SOURCE                    # (also for the test program)
# 16 KB pages (Android 15): the segments are aligned to 16384
EXT_LDFLAGS += -shared -Wl,-z,max-page-size=16384 -Wl,--build-id=none -lm -ldl -llog
EXT_STRIP = $(NDK_BIN)/llvm-strip --strip-unneeded $@
else ifeq ($(PLATFORM),windows)
EXT_SUFFIX := dll
EXT_CFLAGS := $(filter-out -fPIC -fvisibility=hidden,$(EXT_CFLAGS)) -D_FILE_OFFSET_BITS=64 -D__USE_MINGW_ANSI_STDIO=1
CFLAGS += -D_FILE_OFFSET_BITS=64 -D__USE_MINGW_ANSI_STDIO=1
EXT_LDFLAGS += -shared -static-libgcc -Wl,-Bstatic -lpthread -Wl,-Bdynamic
EXT_STRIP = strip --strip-unneeded $@
else ifeq ($(PLATFORM),linux-musl)
EXT_CFLAGS += -D_GNU_SOURCE
EXT_LDFLAGS += -shared -lpthread -lm
else   # linux
EXT_CFLAGS += -D_GNU_SOURCE
EXT_LDFLAGS += -shared -lpthread -lm -ldl
endif

EXT := $(DIST)/$(EXT_BASE).$(EXT_SUFFIX)
EXT_OBJ := $(patsubst $(SRC_DIR)/%.c,$(EXT_BUILD)/%.o,$(ENGINE_SRC))

$(EXT_BUILD)/%.o: $(SRC_DIR)/%.c $(HEADERS)
	@mkdir -p $(EXT_BUILD)
	$(CC) $(EXT_CFLAGS) -c $< -o $@

.PHONY: extension
extension: $(EXT)

ifeq ($(EXT_UNIVERSAL),true)
# Mac Catalyst: a build for each architecture, then one universal file
$(EXT):
	@mkdir -p $(DIST)
	@for arch in x86_64 arm64; do $(MAKE) --no-print-directory extension PLATFORM=mac-catalyst ARCH=$$arch DIST=$(DIST)/catalyst-$$arch || exit 1; done
	lipo -create $(DIST)/catalyst-x86_64/$(EXT_BASE).dylib $(DIST)/catalyst-arm64/$(EXT_BASE).dylib -output $@
	rm -rf $(DIST)/catalyst-x86_64 $(DIST)/catalyst-arm64
	$(EXT_STRIP)
else
$(EXT): $(EXT_OBJ) $(if $(filter windows,$(PLATFORM)),$(EXT_BUILD)/$(EXT_BASE).def)
	@mkdir -p $(DIST)
	$(CC) $^ -o $@ $(EXT_LDFLAGS)
ifeq ($(PLATFORM),windows)
	dlltool -D $(EXT_BASE).dll -d $(EXT_BUILD)/$(EXT_BASE).def -l $(DIST)/$(EXT_BASE).lib
endif
	$(EXT_STRIP)
endif

# Windows: the exported names (the DLL hides everything else)
$(EXT_BUILD)/$(EXT_BASE).def:
	@mkdir -p $(EXT_BUILD)
	@printf 'LIBRARY %s.dll\nEXPORTS\n    sqlite3_multiwriter_init\n    sqlite3_multiwriter_default_init\n    mw_version\n' $(EXT_BASE) > $@

# ---- the test of the extension that was built: loaded into a stock SQLite (the SQLite of the amalgamation without the engine) ----
$(BUILD)/sqlite3_plain.o: $(SQLITE_DIR)/sqlite3.c
	@mkdir -p $(BUILD)
	$(CC) $(CFLAGS) -DSQLITE_THREADSAFE=1 -w -c $< -o $@
$(BUILD)/t_loadable.o: test/loadable.c test/mw_test.h $(HEADERS)
	@mkdir -p $(BUILD)
	$(CC) $(CFLAGS) -c $< -o $@
LOADABLE_LIBS := $(if $(filter windows,$(PLATFORM)),-static) -lpthread -lm $(if $(filter android,$(PLATFORM)),-ldl,$(if $(filter linux linux-musl,$(PLATFORM)),-ldl))
$(DIST)/loadable$(EXE): $(BUILD)/t_loadable.o $(BUILD)/sqlite3_plain.o
	@mkdir -p $(DIST)
	$(CC) $^ -o $@ $(LOADABLE_LIBS)
.PHONY: test-loadable
test-loadable: $(EXT) $(DIST)/loadable$(EXE)
	./$(DIST)/loadable$(EXE) ./$(EXT)
