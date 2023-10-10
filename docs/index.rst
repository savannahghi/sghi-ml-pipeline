.. sghi-commons documentation master file, created by
   sphinx-quickstart on Thu Aug 3 01:28:14 2023.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

.. image:: images/sghi_logo.webp
   :align: center

SGHI ML Pipeline
================

sghi-ml-pipelime is a tool for defining and executing our ML workflows.

Installation
------------

We recommend using the latest version of Python. Python 3.10 and newer is
supported. We also recommend using a `virtual environment`_ in order
to isolate your project dependencies from other projects and the system.

Install the latest sghi-ml-pipeline version using pip:

.. code-block:: bash

    pip install sghi-ml-pipeline


API Reference
-------------

.. autosummary::
   :template: module.rst
   :toctree: api
   :caption: API
   :recursive:

     sghi.ml_pipeline


.. _virtual environment: https://packaging.python.org/tutorials/installing-packages/#creating-virtual-environments
