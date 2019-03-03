# A fragile parser

This is a parser for Charlottesville city council meeting minutes. It depends on having those minutes, preferably
in a `data` directory. It also requires that the metadata for the dates be available -- there's currently a global variable
in `scripts/driver.py` pulling in the timestamp of publication for each meeting minutes file, although that should change soon.

# Dependencies

tika-python
pandas
numpy

# To run
- Clone the repo
- Extract the minutes files from [this](https://console.cloud.google.com/storage/browser/charlottesville-council-minutes) Google bucket to a data directory in the project (TODO -- setup.py this)
- `python driver.py`
