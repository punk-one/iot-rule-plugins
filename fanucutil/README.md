# FANUC FOCAS optional adapter

This directory is an independent optional module and is not imported by any
official rule plugin. Standard builds use the portable unsupported stub.

To enable the proprietary implementation, install the licensed FANUC headers
and shared library under `../fanuc_libs`, verify their provenance and checksum,
then build explicitly with `-tags fanuc` on Linux AMD64.
