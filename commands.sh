#!/usr/bin/env bash
cd $(dirname $0)
sleep 10s
cqlsh -f common/commands.cql
