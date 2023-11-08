# SPDX-FileCopyrightText: 2023-present Dask Developers
#
# SPDX-License-Identifier: BSD-3

from .databrickscluster import DatabricksCluster, get_client  # noqa

# Define the variable '__version__':
try:
    # If setuptools_scm is installed (e.g. in a development environment with
    # an editable install), then use it to determine the version dynamically.
    from setuptools_scm import get_version

    # This will fail with LookupError if the package is not installed in
    # editable mode or if Git is not installed.
    __version__ = get_version(root="..", relative_to=__file__)
except (ImportError, LookupError):
    # As a fallback, use the version that is hard-coded in the file.
    try:
        from dask_databricks._version import __version__  # noqa: F401
    except ModuleNotFoundError:
        # The user is probably trying to run this without having installed
        # the package, so complain.
        raise RuntimeError("dask-databricks is not correctly installed. " "Please install it with pip.")
