#!/usr/bin/env bash

docker run -it --rm $(docker build -q .)
