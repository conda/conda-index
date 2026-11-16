### Other

* Set up Miniconda explicitly in the documentation build instead of using the copy preinstalled on the runner, which the Ubuntu 26.04 images no longer include. (#382)

* Skip PostgreSQL tests in the weekly canary run, which has no PostgreSQL installation, and open or update an issue when that run fails. (#382)
