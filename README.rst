.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015-2019 Cask Data, Inc.

.. image:: https://cdap-users.herokuapp.com/badge.svg?t=1
    :target: https://cdap-users.herokuapp.com

.. image:: https://img.shields.io/badge/License-Apache%202.0-blue.svg
    :target: https://opensource.org/licenses/Apache-2.0

.. image:: https://travis-ci.org/cdapio/cdap.svg
    :target: https://travis-ci.org/cdapio/cdap


Introduction
============

CDAP is an integrated, open source application
development platform for the Hadoop ecosystem that provides developers with data and
application abstractions to simplify and accelerate application development, address a
broader range of real-time and batch use cases, and deploy applications into production
while satisfying enterprise requirements.

CDAP is a layer of software running on top of Apache Hadoop® platforms such as the
Cloudera Enterprise Data Hub or the Hortonworks® Data Platform. CDAP provides these
essential capabilities:

- Abstraction of data in the Hadoop environment through logical representations of underlying data;
- Portability of applications through decoupling underlying infrastructures;
- Services and tools that enable faster application creation in development;
- Integration of the components of the Hadoop ecosystem into a single platform;
- Metadata management that automatically captures metadata and lineage;
- CDAP pipelines with an integrated UI for click-and-drag development; and
- Higher degrees of operational control in production through enterprise best-practices.

CDAP exposes developer APIs (Application Programming Interfaces) for creating applications
and accessing core CDAP services. CDAP defines and implements a diverse collection of
services that land applications and data on existing Hadoop infrastructure such as HBase,
HDFS, YARN, MapReduce, Hive, and Spark.

You can run applications ranging from simple MapReduce Jobs and complete ETL (extract,
transform, and load) pipelines all the way up to complex, enterprise-scale data-intensive
applications.

Developers can build and test their applications end-to-end in a full-stack, single-node
installation. CDAP can be run either as a Sandbox, deployed within the Enterprise
on-premises or hosted in the Cloud.

For more information, see our collection of `Developers' Manual and other documentation
<http://docs.cask.co/cdap/current/en/developers-manual/index.html>`__.


Getting Started
===============

Prerequisites
-------------

To install and use CDAP, there are a few simple prerequisites:

1. JDK 8+ (required to run CDAP; note that $JAVA_HOME should be set)
#. `Node.js <https://nodejs.org/>`__ (required to run the CDAP UI; we recommend any version greater than v4.5.0)
#. Apache Maven 3.0+ (required to build the example applications; 3.1+ to build CDAP itself)

Build
-----

You can get started with CDAP by building directly from the latest source code::

  git clone https://github.com/caskdata/cdap.git
  cd cdap
  git submodule update --init --recursive --remote
  mvn clean package

After the build completes, you will have built all modules for CDAP.

For more build options, please refer to the `build instructions <BUILD.rst>`__.


Introductory Tutorial
=====================

Visit our web site for an `introductory tutorial for developers
<http://docs.cask.co/cdap/current/en/developers-manual/getting-started/index.html>`__ that
will guide you through installing CDAP and running an example application.


Where to Go Next
================

Now that you've had a look at the CDAP Sandbox, take a look at:

- Developers' Manual, located in the source distribution in ``cdap-docs/developers-manual/source``
  or `online <http://docs.cask.co/cdap/current/en/developers-manual/index.html>`__.
- `CDAP Releases and timeline <http://docs.cask.co/cdap/index.html>`__


How to Contribute
=================

Interested in helping to improve CDAP? We welcome all contributions, whether in filing
detailed bug reports, submitting pull requests for code changes and improvements, or by
asking questions and assisting others on the mailing list.

For quick guide to getting your system setup to contribute to CDAP, take a look at our
`Contributor Quickstart Guide <DEVELOPERS.rst>`__.

Filing Issues: Bug Reports & Feature Requests
---------------------------------------------
Bugs and suggestions should be made by `filing an issue <https://issues.cask.co/browse/cdap>`__.

Existing issues can be browsed at `the CDAP project issues
<https://issues.cask.co/browse/CDAP-8373?jql=project%20%3D%20CDAP>`__.

Pull Requests
-------------

We have a simple pull-based development model with a consensus-building phase, similar to
Apache's voting process. If you’d like to help make CDAP better by adding new features,
enhancing existing features, or fixing bugs, here's how to do it:

1. If you are planning a large change or contribution, discuss your plans on the
   `cdap-dev@googlegroups.com <https://groups.google.com/d/forum/cdap-dev>`__ mailing list first.
   This will help us understand your needs and best guide your solution in a way that fits the project.
2. Fork CDAP into your own GitHub repository.
3. Create a topic branch with an appropriate name.
4. Work on the code to your heart's content.
5. Once you’re satisfied, create a pull request from your GitHub repo (it’s helpful if you fill in
   all of the description fields).
6. After we review and accept your request, we’ll commit your code to the caskdata/cdap repository.

Thanks for helping to improve CDAP!

Mailing Lists
-------------

CDAP User Group and Development Discussions:

- `cdap-user@googlegroups.com <https://groups.google.com/d/forum/cdap-user>`__

The *cdap-user* mailing list is primarily for users using the product to develop
applications. You can expect questions from users, release announcements, and any other
discussions that we think will be helpful to the users.

- `cdap-dev@googlegroups.com <https://groups.google.com/d/forum/cdap-dev>`__

The *cdap-dev* mailing list is essentially for developers actively working
on the product, and should be used for all our design, architecture and technical
discussions moving forward. This mailing list will also receive all JIRA and GitHub
notifications.


License and Trademarks
======================

Copyright © 2014-2017 Cask Data, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the
License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
either express or implied. See the License for the specific language governing permissions
and limitations under the License.

Cask is a trademark of Cask Data, Inc. All rights reserved.

Apache, Apache HBase, and HBase are trademarks of The Apache Software Foundation. Used with
permission. No endorsement by The Apache Software Foundation is implied by the use of these marks.
