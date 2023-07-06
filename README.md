<!-- # Kuduraft with [AirReplay](https://github.com/Ngalstyan4/AirReplay) integration -->

## Building Kudu out of tree

A single Kudu source tree may be used for multiple builds, each with its
own build directory. Build directories may be placed anywhere in the
filesystem with the exception of the root directory of the source tree. The
Kudu build is invoked with a working directory of the build directory
itself, so you must ensure it exists (i.e. create it with _mkdir -p_). It's
recommended to place all build directories within the _build_ subdirectory;
_build/latest_ will be symlinked to most recently created one.

The rest of this document assumes the build directory
_<root directory of kudu source tree>/build/debug_.

### Automatic rebuilding of dependencies

The script `thirdparty/build-if-necessary.sh` is invoked by cmake, so
new thirdparty dependencies added by other developers will be downloaded
and built automatically in subsequent builds if necessary.

To disable the automatic invocation of `build-if-necessary.sh`, set the
`NO_REBUILD_THIRDPARTY` environment variable:

```bash
$ cd build/debug
$ NO_REBUILD_THIRDPARTY=1 cmake ../..
```
### Building Kudu itself

#### Prerequisites
```bash
sudo apt update
sudo apt install cmake g++ autoconf libtoolize pkg-config flex unzip libsasl2-dev libssl-dev
```

### N.B change cmake dir before building
Add `<root of kudu tree>/thirdparty/installed/common/bin` to your `$PATH`
before other parts of `$PATH` that may contain cmake, such as `/usr/bin`
For example:

 `"export PATH=$HOME/kudu_workspace/kuduraft/thirdparty/installed/common/bin:$PATH"`

```bash
mkdir -p build/debug
cd build/debug
cmake ../..
make -j30  # or whatever level of parallelism your machine can handle
```

The build artifacts, including the test binaries, will be stored in
_build/debug/bin/_.

To omit the Kudu unit tests during the build, add -DNO_TESTS=1 to the
invocation of cmake. For example:

```bash
cd build/debug
cmake -DNO_TESTS=1 ../..
```


## Testing AirReplay Record-Replay

First, build the kudu-tserver binary with AirReplay
```bash
INSTALLED_BIN_DIR="$HOME/kudu_workspace/kuduraft/thirdparty/installed/common/bin" # <-- change if necessary
export PATH="$INSTALLED_BIN_DIR:$PATH"
mkdir -p build/debug
cd build/debug
cmake ../..
make kudu-tserver -j44

ln -s ../../full_test.sh .
ln -s ../../Procfile .
```

Then, run the `full_test.sh` test suite
```bash
./full_test.sh
```

The script

1. Builds `kudu-tserver` again if source have changed
2. Sets up necessary configs under `./cluster` for a 3 node kuduraft cluseter
3. Runs a 3 node kuduraft cluster under AirReplay recording in 3 processes on the same machine using goreman
4. Waits till `TIMEOUT` and kills the cluster.
5. Asks the user whether to replay the recorded trace.
6. If confirmed, replays each individual node one after another using AirReplay in replay mode