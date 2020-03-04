#!/usr/bin/env bash
cd tcomextdata/tasks && exec python -m luigi --module "$@"

