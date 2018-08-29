# Tools

The `tools/` directory is where we put image builds for the various tools used during development.

To manually build the tools, the `build.sh` script can be used.

    $ tools/build.sh tools/pigeon

Alternatively, the `docker build` can be called directly.

    $ docker build -t quay.io/influxdb/<dirname>:$version

The version tag is meant to be the value in the `VERSION` file within the directory and the image name is the directory name.

When a tool is updated here, be sure that the relevant build script in `build-scripts` is also updated to reflect the version change.
