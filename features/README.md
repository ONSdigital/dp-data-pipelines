# `enviroment` setup

This scipt sets up an enviorment (docker container) for testing and after testing has been completed removes the container and temporary directories.

## `before_all` function

The script sets up a container representing a fake backend to run tests. These test confirms that the request are routing as required, also checks the fixtures unzipped data paths are correct/exist.

## `before_scenario` function

This function will run bfeore each scenario would be ran. Creates a temporary directory and changes current working directory to the new temporary directory. This allows test files to be placed in there and later safely can be deleted.

Set the `UUID` (Universal Unique Identifier) in the docker logs.
Sets each scenario with a `custom Session` so some of the default set headers will be removed.

## `after_scenario` function

This function removes the temporary directories and all contents, then changes directory out of the temporary directory.

## `after_all` function

This function stops and removes the `docker container`, also remove temporary directories for test output files. 