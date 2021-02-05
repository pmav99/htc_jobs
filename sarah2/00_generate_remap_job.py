#!/usr/bin/env python3

from __future__ import annotations

import argparse
import pathlib
import shutil

import htclib

parser = argparse.ArgumentParser(description="Generate a Remap job for a single year")
parser.add_argument('image_type', type=str, choices=["SID", "SIS"])
parser.add_argument('year', type=int)
args = parser.parse_args()


ROOT_DIR = pathlib.Path(__name__).parent.resolve()
JOB_DIR = ROOT_DIR / f"jobs/00_{args.image_type}_{args.year}_remap"
JOB_DIR.mkdir(parents=True, exist_ok=True)
OUT_DIR = ROOT_DIR / f"outs/00_{args.image_type}_{args.year}_remap"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def copy_gridfile():
    shutil.copy(ROOT_DIR / "gridfile.txt", JOB_DIR)


def get_input_files(image_type: str, year: int):
    return list(pathlib.Path("/eos/jeodpp/data/projects/PVGIS/meteo/sarah2/in/").glob(f"{image_type}in{year}*.nc"))


def get_output_files(input_files):
    return [OUT_DIR / ifile.name for ifile in input_files]


def write_csv(input_files, output_files):
    contents = "\n".join(",".join((ifile.as_posix(), ofile.as_posix())) for (ifile, ofile) in zip(input_files, output_files))
    (JOB_DIR / "arguments.csv").write_text(contents)


def create_job():
    job = htclib.HTCondorJob(
        path=JOB_DIR / "job.txt",
        proc_user="pvgisproc",
        docker_image="alexgleith/cdo",
        memory="3G",
        log_prefix=JOB_DIR / "logs",
        executable="/usr/local/bin/cdo",
        arguments="-O -z zip -seltimestep,1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31,33,35,37,39,41,43,45,47 -remapbil,gridfile.txt $(input_file) $(output_file)",
        input_files="./gridfile.txt",
        queue=f"input_file,output_file from {JOB_DIR / 'arguments.csv'}",
    )
    print(job)
    return job


def main(args):
    copy_gridfile()
    input_files = get_input_files(args.image_type, args.year)
    output_files = get_output_files(input_files)
    write_csv(input_files, output_files)
    job = create_job()
    htclib.save_job(job)


if __name__ == "__main__":
    main(args)
