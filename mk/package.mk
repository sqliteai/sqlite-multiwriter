# Packaging (included by the Makefile): the archive of a platform, the Apple XCFramework, the Android AAR.

ARCH_TAG := $(if $(ARCH),$(ARCH),$(if $(filter macos,$(PLATFORM)),universal,$(if $(filter ios-sim mac-catalyst,$(PLATFORM)),universal,$(shell uname -m))))
PKG_NAME := multiwriter-$(PLATFORM)-$(ARCH_TAG)-$(VERSION)

# make package: dist/multiwriter-<platform>-<arch>-<version>.tar.gz (and .zip) with the extension, the license and the notice
.PHONY: package
package: $(EXT)
	@rm -rf $(DIST)/$(PKG_NAME) && mkdir -p $(DIST)/$(PKG_NAME)
	cp $(EXT) LICENSE NOTICE $(DIST)/$(PKG_NAME)/
ifeq ($(PLATFORM),windows)
	cp $(DIST)/$(EXT_BASE).lib $(DIST)/$(PKG_NAME)/ 2>/dev/null || true
endif
	cd $(DIST) && tar -czf $(PKG_NAME).tar.gz $(PKG_NAME) && (zip -rq $(PKG_NAME).zip $(PKG_NAME) 2>/dev/null || true)
	rm -rf $(DIST)/$(PKG_NAME)

# ---- Apple XCFramework: MultiWriter.xcframework with a framework for iOS, the iOS simulator, Mac Catalyst and macOS (needs Xcode) ----
FMWK := MultiWriter
define PLIST
<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\
<plist version=\"1.0\">\
<dict>\
<key>CFBundleDevelopmentRegion</key>\
<string>en</string>\
<key>CFBundleExecutable</key>\
<string>$(FMWK)</string>\
<key>CFBundleIdentifier</key>\
<string>io.sqlitecloud.multiwriter</string>\
<key>CFBundleInfoDictionaryVersion</key>\
<string>6.0</string>\
<key>CFBundleName</key>\
<string>$(FMWK)</string>\
<key>CFBundlePackageType</key>\
<string>FMWK</string>\
<key>CFBundleSignature</key>\
<string>????</string>\
<key>CFBundleVersion</key>\
<string>$(VERSION)</string>\
<key>CFBundleShortVersionString</key>\
<string>$(VERSION)</string>\
<key>MinimumOSVersion</key>\
<string>11.0</string>\
</dict>\
</plist>
endef

define MODULEMAP
framework module $(FMWK) {\
  umbrella header \"$(FMWK).h\"\
  export *\
}
endef

# the libraries to put in frameworks, in the order of FMWK_DIRS (the last one, macOS, is a "deep" framework with Versions/A, as macOS wants it)
XC_PLATFORMS := ios ios-sim mac-catalyst macos
XC_DIRS := ios-arm64 ios-arm64_x86_64-simulator ios-arm64_x86_64-maccatalyst macos-arm64_x86_64
XC_OUT := $(DIST)/$(FMWK).xcframework

$(XC_OUT):
	rm -rf $(BUILD)/xc $(XC_OUT) && mkdir -p $(BUILD)/xc
	@for p in $(XC_PLATFORMS); do \
		$(MAKE) --no-print-directory extension PLATFORM=$$p DIST=$(BUILD)/xc/$$p BUILD=$(BUILD)/xc/obj-$$p || exit 1; \
	done
	@set -e; i=0; for p in $(XC_PLATFORMS); do \
		i=$$((i+1)); dir=$$(echo $(XC_DIRS) | cut -d' ' -f$$i); f=$(DIST)/$$dir/$(FMWK).framework; \
		rm -rf $(DIST)/$$dir; \
		if [ $$p = macos ]; then \
			mkdir -p $$f/Versions/A/Headers $$f/Versions/A/Modules $$f/Versions/A/Resources; \
			sed 's/@VERSION@/$(VERSION)/' packages/apple/$(FMWK).h > $$f/Versions/A/Headers/$(FMWK).h; \
			printf "$(PLIST)" > $$f/Versions/A/Resources/Info.plist; \
			printf "$(MODULEMAP)" > $$f/Versions/A/Modules/module.modulemap; \
			cp $(BUILD)/xc/$$p/$(EXT_BASE).dylib $$f/Versions/A/$(FMWK); \
			install_name_tool -id "@rpath/$(FMWK).framework/Versions/A/$(FMWK)" $$f/Versions/A/$(FMWK); \
			ln -sf A $$f/Versions/Current; ln -sf Versions/Current/$(FMWK) $$f/$(FMWK); ln -sf Versions/Current/Headers $$f/Headers; \
			ln -sf Versions/Current/Modules $$f/Modules; ln -sf Versions/Current/Resources $$f/Resources; \
		else \
			mkdir -p $$f/Headers $$f/Modules; \
			sed 's/@VERSION@/$(VERSION)/' packages/apple/$(FMWK).h > $$f/Headers/$(FMWK).h; \
			printf "$(PLIST)" > $$f/Info.plist; \
			printf "$(MODULEMAP)" > $$f/Modules/module.modulemap; \
			cp $(BUILD)/xc/$$p/$(EXT_BASE).dylib $$f/$(FMWK); \
			install_name_tool -id "@rpath/$(FMWK).framework/$(FMWK)" $$f/$(FMWK); \
		fi; \
	done
	xcodebuild -create-xcframework $(foreach d,$(XC_DIRS),-framework $(DIST)/$(d)/$(FMWK).framework) -output $(XC_OUT)
	rm -rf $(addprefix $(DIST)/,$(XC_DIRS)) $(BUILD)/xc

.PHONY: xcframework
xcframework: $(XC_OUT)

# ---- Android AAR: packages/android (a gradle library) with the extension of the four ABIs as jniLibs (needs ANDROID_NDK, and gradle) ----
AAR_ABIS := arm64-v8a armeabi-v7a x86_64 x86
AAR_JNI := packages/android/src/main/jniLibs
.PHONY: aar
aar:
	@set -e; for abi in $(AAR_ABIS); do \
		mkdir -p $(AAR_JNI)/$$abi; \
		$(MAKE) --no-print-directory extension PLATFORM=android ARCH=$$abi DIST=$(BUILD)/aar/$$abi BUILD=$(BUILD)/aar/obj-$$abi; \
		cp $(BUILD)/aar/$$abi/$(EXT_BASE).so $(AAR_JNI)/$$abi/lib$(EXT_BASE).so; \
	done
	cd packages/android && gradle --no-daemon -PVERSION=$(VERSION) clean assembleRelease
	mkdir -p $(DIST) && cp packages/android/build/outputs/aar/android-release.aar $(DIST)/$(EXT_BASE).aar
	rm -rf $(BUILD)/aar $(AAR_JNI)
