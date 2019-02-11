#!/bin/bash
impala-shell.sh -q "create database if not exists go_impala_repro"
for i in $(seq 2000); do
  impala-shell.sh -q "create table if not exists go_impala_repro.foo${i} (i int)"
done
