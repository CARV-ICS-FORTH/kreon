# Kreon

Kreon is a key-value store library optimized for flash-based storage, where CPU overhead and I/O amplification are more significant bottlenecks compared to I/O randomness.

![Kreon](https://i.imgur.com/Jd1nCepm.png)

In typical key-value stores LSM-Trees introduce increasing I/O amplification as their size grows, which decreases overall performance. On the other hand, Kreon stores key-value pairs in a sequential value log (WAL) and keeps an index with their corresponding locations. The index is organized as an LSM-Tree of value pointers, with multiple levels of increasing size, where each level `i` acts as a buffer for the next level `i+1`. To reduce I/O amplification when the metadata is merged at each level, Kreon maintains a B+-tree index per level.

The following are available after building and installing:

* `kreon.h`, `libkreon.{a,so}`: Kreon library (static, dynamic version depending on build options)
* `mkfs.kreon`: Utility to format block device or file for Kreon
* `ycsb-edb` - Standalone Kreon ycsb benchmark

## Building

_The following instructions have been verified in CentOS 7._

Install dependencies:
```sh
sudo yum install cmake3 kernel-devel gcc-c++ numactl-devel boost-devel devtoolset-7-gcc
```

Configure:
```sh
scl enable devtoolset-7 /bin/bash
mkdir build
cd build
cmake3 ..
make
```

The CMake scripts provided support two build configurations: "Release" and "Debug" (default). The "Debug" configuration enables the `-g` option during compilation to allow debugging. The "Release" build disables warnings and enables optimizations. The build configuration can be defined as a parameter to the cmake call as follows:
```sh
cmake3 -DCMAKE_BUILD_TYPE=Debug|Release ..
```

Kreon can be built as a static (default) or dynamic library. Add `-DBUILD_SHARED_LIBS=ON` to the `cmake` command to build the dynamic version.

## Generated Files

After `make all` is executed successfully, the following files and folders are produced:

* `kreon_lib` contains the dynamic and Kreon library and `mkfs.kreon`. `mkfs.kreon` takes 4 arguments the file or device name, the device offset and the device size and formats it appropriately in order for Kreon to open and write data in it.
* `YCSB-CXX` is a C++ version of the YCSB benchmark. It contains ycsb-edb which is YCSB linked with Kreon ready to run and a number of helper scripts.

`ycsb-edb` takes 4 arguments `-threads`, `-dbnum`, `-path` and `-e`:

* `threads` is the number of threads the YCSB benchmark will spawn to perform operations.
* `dbnum` is the number of regions that will open in Kreon at the initialization phase of YCSB.
* `path` is the file or device that will open to store the data generated from the workloads.
* `e` is the execution plan with workloads YCSB will run.

When YCSB finishes a `RESULTS` folder is created that contains all the metrics each workload produced.

To run YCSB with Kreon you need to run the following commands:
```sh
./mkfs.kreon.single.sh /tmp/kreon.dat 1 1
./ycsb-edb -threads 10 -dbnum 10 -e execution_plan.txt -path /tmp/kreon.dat
```

`mkfs.kreon.single.sh` is a script that takes the file or device name and initializes it. The first argument is the device name. The second is the number of regions and is obsolete set it to 1. Finally the third argument informs the script if the given path is a file or device. Set it to 1 for file and 0 for device.

## Installing and packaging

Run `make install` inside the `build` folder to install the header and libraries.

Run `make uninstall` inside the `build` folder to remove files installed by `make install` (directories are not deleted).

Run `make package` inside the `build` folder to create an RPM file.

## Acknowledgements

Kreon uses modified versions of the following projects:

* [min-heap](https://github.com/robin-thomas/min-heap)
* [YCSB-C](https://github.com/basicthinker/YCSB-C)
* [log](https://github.com/innerout/log.c)

This project has received funding from the European Union’s Horizon 2020 research and innovation programme under grant agreement No 825061 (EVOLVE - [website](https://www.evolve-h2020.eu), [CORDIS](https://cordis.europa.eu/project/id/825061)).
