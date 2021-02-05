import logging
import pathlib

from typing import List

import htclib

logger = logging.getLogger()

# User input
GRIDFILE = pathlib.Path(__file__).resolve().parent / "gridfile.txt"
TIMESTEPS = "1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31,33,35,37,39,41,43,45,47"
PREFIX = "SID_per_day"

BASE_INPUT_DIR = pathlib.Path("/eos/jeodpp/data/projects/PVGIS/meteo/sarah2/in_re/")
BASE_OUTPUT_DIR = pathlib.Path(f"/eos/jeodpp/data/projects/PVGIS/mavropa/outs/{PREFIX}")
BASE_JOB_DIR = pathlib.Path(f"/eos/jeodpp/data/projects/PVGIS/mavropa/jobs/{PREFIX}")

# Constants
# This is the base argument command. We need to append the input file and the output file
# but we will do this in `create_job()`
CONDOR_BASE_ARGUMENTS = f"-z zip -O -remapbil,{GRIDFILE.as_posix()} -seltimestep,{TIMESTEPS}"

# The common arguments for the jobs are
COMMON_OPTIONS = dict(
    proc_user="pvgisproc",
    docker_image="alexgleith/cdo",
    executable="/usr/local/bin/cdo",
    memory="4G",
)


def get_input_files_per_year(base_dir: pathlib.Path, year: int) -> List[pathlib.Path]:
    """
    Return a list with the paths to the files that match `pattern`.

    E.g. to get the files for 2005 we would use something like this:

        get_input_files(
            base_dir=pathlib.Path("/eos/jeodpp/data/projects/PVGIS/meteo/sarah2/in_re/")),
            year="2005",
        )

    """
    pattern = f"SIDin{year}*.nc"
    return list(base_dir.glob(pattern))


def get_output_files_per_year(base_dir: pathlib.Path, year: int, input_files: List[pathlib.Path]) -> List[pathlib.Path]:
    output_dir = base_dir / str(year)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_files = [output_dir / input_file.name for input_file in input_files]
    return output_files


def get_job_files_per_year(base_dir: pathlib.Path, year: int, input_files: List[pathlib.Path]) -> List[pathlib.Path]:
    job_dir = base_dir / str(year)
    job_dir.mkdir(parents=True, exist_ok=True)
    job_files = [job_dir / (input_file.stem + ".job") for input_file in input_files]
    return job_files


def create_job(job_file: pathlib.Path, input_file: pathlib.Path, output_file: pathlib.Path) -> htclib.HTCondorJob:
    """
    Create and return an HTCondorJob object

    Arguments
    ---------

    job_file: The path to the job description/submission file
    input_file: The path to the NetCDF file we want to process
    output_file: The path where we want to save the output NetCDF file

    """
    arguments = " ".join([CONDOR_BASE_ARGUMENTS, input_file.as_posix(), output_file.as_posix()])
    job = htclib.HTCondorJob(path=job_file, arguments=arguments, **COMMON_OPTIONS)
    return job


def process_year(
    base_input_dir: pathlib.Path,
    base_output_dir: pathlib.Path,
    base_job_dir: pathlib.Path,
    year: int
) -> None:
    input_files = get_input_files_per_year(base_dir=base_input_dir, year=year)
    output_files = get_output_files_per_year(base_dir=base_output_dir, input_files=input_files, year=year)
    job_files = get_job_files_per_year(base_dir=base_job_dir, input_files=input_files, year=year)
    # Generate job objects
    jobs = []
    for job_file, input_file, output_file in zip(job_files, input_files, output_files):
        jobs.append(create_job(job_file=job_file, input_file=input_file, output_file=output_file))
    logger.info("Job creation: OK")
    # save job submissions files to disk
    for job in jobs:
        htclib.save_job(job)
    logger.info("Job saving: OK")
    # submit jobs
    for job in jobs:
        htclib.submit_job(job)
    logger.info("Job submission: OK")


def main():
    for year in range(2005, 2010):
        logger.info("Starting year: %d", year)
        process_year(
            base_input_dir=BASE_INPUT_DIR,
            base_output_dir=BASE_OUTPUT_DIR,
            base_job_dir=BASE_JOB_DIR,
            year=year,
        )
        logger.info("Finished year: %d", year)


if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s; %(levelname)-8s; %(name)-35s; %(funcName)-20s;%(lineno)4d: %(message)s",
        level=logging.INFO,
    )
    main()
