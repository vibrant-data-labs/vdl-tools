# VDL Tools

This repository contains a set of tools which are used at Vibrant Data Labs.

## Tools

- [Network Tools](./vdl_tools/network-tools/)
- [LinkedIn](./vdl_tools/LinkedIn/) - Set of methods to interact with LinkedIn via CoreSignal API
- [Download Process Images](./vdl_tools/download_process_images) - Helper methods to download, convert and save images to S3 bucket
- [Py2Mappr](./vdl_tools/py2mappr) - Python wrapper for generating the OpenMappr player
- [Scrape Enrich](./vdl_tools/scrape_enrich/)
- [Tag2Network](./vdl_tools/tag2network/)
- [Shared Tools](./vdl_tools/shared_tools/)

## Installing a new package

The package is intended to work with `pip`, however `pip` does not support adding package reference to `pyproject.toml`. So that the most reliable way to keep the dependencies in sync is to:

1. Add the package to `pyproject.toml` file, to the `[dependencies]` section
2. Run `pip install -e .` to install the package in editable mode, which will trigger the installation of the dependencies
