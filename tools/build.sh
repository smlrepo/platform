#!/bin/bash

set -e

if [ $# -eq 0 ]; then
  echo 1>&2 "At least one directory must be specified."
  exit 1
fi

for dir in "$@"; do
  if [ ! -e "$dir/Dockerfile" ]; then
    echo 1>&2 "$dir: Dockerfile is not present"
    exit 1
  fi
  if [ ! -e "$dir/VERSION" ]; then
    echo 1>&2 "$dir: VERSION file is not present"
    exit 1
  fi
  docker build -t quay.io/influxdb/$(basename "$dir"):$(cat "$dir/VERSION") "$dir"
done
