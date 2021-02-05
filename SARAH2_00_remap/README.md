In order to do the remapping we need to run a command like this:

    cdo \
      -O \
      -z zip \
      -remap,gridfile.txt \
      -seltimestep,1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31,33,35,37,39,41,43,45,47 \
      /path/to/input_file.nc \
      /path/to/output_file.nc

The SARAH2 files contain two observations per hour. E.g. 00:00, 00:30, 01:00, 01:30, etc
We are only interested in one observations. I.e. 00:00, 01:00, 01:00 etc
we drop the observations we don't care about with the `seltimestep` line

More info about the remapping procedure can be found in the following links:

- http://www.climate-cryosphere.org/wiki/index.php?title=Regridding_with_CDO
