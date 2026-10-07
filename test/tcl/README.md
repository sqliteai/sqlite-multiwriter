# SQLite's own Tcl test suite through the engine

`/tmp`-based recipe (nothing here is downloaded by the build; the SQLite source tree is only needed for this experiment):

1. Download `sqlite-src-3530400.zip` (same version as `third_party/sqlite`), unzip it, `mkdir bld && cd bld && ../sqlite-src-3530400/configure --all --with-tcl=<Tcl.framework dir of the macOS SDK>`, `make testfixture`.
2. Build `testfixture_mw`: the same compile line as `make -n testfixture` plus `-DSQLITE_EXTRA_INIT=mw_extra_init -I<repo>/src`, `<repo>/src/*.c` as extra sources and `-framework Security`.
3. `MW_DEFAULT_MODE=2 python3 run.py ./testfixture_mw out.json 60 8` (every database opens with `mw=2`; `run.py BIN OUT timeout jobs` runs each `test/*.test` in its own directory), the same with the stock `testfixture` for the baseline. `one.sh NAME` shows the first failures of one test.

Results and their reading: `docs/sqlite-test-suite.md`.
