#!/usr/bin/env bash
# hostname command on andes compute node returns a full domain name, which
# is different from $SLURM_NODELIST. Use this script to make the hostname
# consistent for magpie.

if [ -n "$1" ]; then
  echo "$1" | cut -d'.' -f1
  exit 0
fi

exit 1

