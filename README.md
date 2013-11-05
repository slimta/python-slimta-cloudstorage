#### [Project Homepage][1]
#### [API Documentation][2] and [Manual][3]

--------------------

About
=====

This extension provides a queue storage mechanism that interfaces with two
common cloud service providers: Rackspace and Amazon Web Services. For
Rackspace, [`pyrax`][4] is used to connect to the Cloud Queues and Cloud Files
services. For AWS, [`boto`][5] is used to connect to S3 and SQS.

[![Build Status](http://ci.slimta.org/job/python-slimta-cloudstorage/badge/icon)](http://ci.slimta.org/job/python-slimta-cloudstorage/)

Getting Started
===============

If you haven't yet installed [`python-slimta`][6], refer to the "Getting
Started" section. Once inside your virtualenv:

    (.venv)$ python setup.py develop

To run the suite of included unit tests:

    (.venv)$ nosetests

Refer to the [API Documentation][2] and [Manual][3] for more information on
using this extension.

[1]: http://slimta.org/
[2]: http://docs.slimta.org/latest/api/extra.cloudstorage.html
[3]: http://docs.slimta.org/latest/manual/extensions.html#cloud-storage
[4]: https://github.com/rackspace/pyrax
[5]: http://aws.amazon.com/sdkforpython/
[6]: https://github.com/slimta/python-slimta

