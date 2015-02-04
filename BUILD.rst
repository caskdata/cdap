=====================================
Cask Data Application Platform - CDAP
=====================================

**Standalone and Distributed CDAP**

Building CDAP Maven
===================

Clean all modules
-----------------

  mvn clean

Run all tests, fail at the end
------------------------------

  MAVEN_OPTS="-Xmx1024m -XX:MaxPermSize=128m" mvn test -fae
    
Build all modules
-----------------

  mvn package

Run checkstyle, skipping tests
------------------------------

  mvn package -DskipTests

Build a particular module
-------------------------

  mvn package -pl [module] -am

Run selected test
-----------------

  MAVEN_OPTS="-Xmx1024m -XX:MaxPermSize=128m" mvn -Dtest=TestClass,TestMore*Class,TestClassMethod#methodName -DfailIfNoTests=false test

See `Surefire doc <http://maven.apache.org/surefire/maven-surefire-plugin/examples/single-test.html`__ for more details

Build all examples
------------------

  MAVEN_OPTS="-Xmx512m" mvn package -DskipTests -pl cdap-examples -am -amd -P examples

Build Standalone distribution ZIP
---------------------------------

  MAVEN_OPTS="-Xmx512m" mvn clean package -DskipTests -P examples -pl cdap-examples -am -amd && mvn package -pl cdap-standalone -am -DskipTests -P dist,release
    
Build the limited set of Javadocs used in distribution ZIP
----------------------------------------------------------

  mvn clean package javadoc:javadoc -pl cdap-api -am -DskipTests -P release

Build the complete set of Javadocs, for all modules
---------------------------------------------------

  mvn clean site -DskipTests
    
Build distributions (rpm, deb, tgz)
-----------------------------------

  mvn package -DskipTests -P dist,rpm-prepare,rpm,deb-prepare,deb,tgz

Show dependency tree
--------------------

  mvn package dependency:tree -DskipTests

Show dependency tree for a particular module
--------------------------------------------

  mvn package dependency:tree -DskipTests -pl [module] -am

Show test output to stdout
--------------------------

  mvn -Dsurefire.redirectTestOutputToFile=false ...

Offline mode
------------

  mvn -o ....

Change version
--------------

  mvn versions:set -DnewVersion=[new_version] -DgenerateBackupPoms=false -P examples


Description of Modules
======================

cdap-common
-----------
Contains code common to all CDAP subsystems.


cdap-explore
------------

About the hive-exec patch file:

This patch file contains all the changes that had to be made to hive branch
`release-0.13.0-rc2` to make it work with CDAP in standalone and in unit-tests. A
hive-exec-0.13.0.jar is already uploaded to Nexus and is used by CDAP in standalone and
for unit tests.

To modify the hive-exec jar again, follow these steps:

1. Clone the Hive repository from Github and checkout to branch *release-0.13.0-rc2*
2. Create another branch: ``git checkout -b my_patched_branch``
3. Apply the existing patch:
   ``git apply --stat cdap-explore/hive-exec-0.13.0.patch``
4. Make your changes in the ``ql`` module (it will be packaged as hive-exec.jar)
5. You can modify ``pom.xml`` in module ``ql`` to change the version number of the 
   generated jar, according to what version of CDAP you are modifying.
6. Deploy your jar to Nexus using this command in the ``ql`` directory:
   ``mvn clean deploy -DskipTests -P hadoop-2 -P sources``
7. Change your dependencies in CDAP to point to the same version number you specified in
   step 5 for hive-exec.
8. Don't forget to create a new patch file and to put it here:
   ``git format-patch release-0.13.0-rc2 --stdout > cdap-explore/hive-exec-0.13.0.patch``

cdap-gateway
------------
CDAP Router

cdap-integration-test
---------------------
CDAP Integration Test Framework

Users can use `IntegrationTestBase` to write tests that run against a framework-provided standalone CDAP instance
or a remote CDAP instance.

Running tests using the framework-provided standalone CDAP instance::

  cd <your-test-module>
  mvn test

Running tests against a remote CDAP instance::

  cd <your-test-module>
  mvn test -DargLine="-DinstanceUri=<instance-uri> -DaccessToken=<access-token>"

where:

- ``<instance-uri>`` is the URI used to connect to your CDAP router (e.g. http://example.com:10000)
- ``<access-token>`` is the access token obtained from your CDAP authentication server by logging in as user
  *Note:* This is unnecessary in a non-secure CDAP instance

For example, to run tests against a CDAP instance at `http://example.com:10000` with access token `abc123`::

  mvn test -DargLine="-DinstanceUri=http://example.com:10000 -DaccessToken=abc123"

cdap-standalone
--------------
Standalone Development Kit files

cdap-unit-test
--------------
Unit-test Framework

cdap-watchdog
-------------
Metrics, Logging, and Alerting





  




    
License and Trademarks
======================

Copyright © 2014-2015 Cask Data, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
in compliance with the License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the 
License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
either express or implied. See the License for the specific language governing permissions 
and limitations under the License.

Cask is a trademark of Cask Data, Inc. All rights reserved.
