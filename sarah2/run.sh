#!/usr/bin/env bash
#

set -xeuo pipefail


image_type='SIS'

for year in {2005..2019}; do
  echo python 00_generate_remap_job.py "${image_type}" "${year}";
done


for year in {2005..2019}; do
  echo submit -batch-name "${image_type}"-"${year}" jobs/00_"${image_type}"_"${year}"_remap/job.txt;
done

query
