=================================
CDAP Contributor Quickstart Guide
=================================

This guide outlines the steps to setup your system for contributing to CDAP.


Prerequisites
=============

- Java 7+ SDK
- Maven 3.1+
- Git


Setup for IntelliJ IDEA
=======================

First, open the CDAP project in IntelliJ IDEA: ``Select File > Open... > cdap/pom.xml``

Then, configure a run configuration to run CDAP Sandbox:

- Select Run > Edit Configurations...
- Add a new "Application" run configuration
- Set "Main class" to be ``io.cdap.cdap.StandaloneMain``
- Set "VM options" to ``-Xmx1024m`` (for in-memory Map/Reduce jobs)
- Click "OK"

You can now use this run configuration to start an instance of CDAP Sandbox.


Build and Run CDAP Sandbox in a Docker Container (Recommended)
==============================================================

These instructions assume the user has a working installation of Docker and a working
understanding of Docker behavior and commands.

- Obtain a fresh copy of the CDAP (GitHub) repo::

    git clone git@github.com:cdapio/cdap.git

- Build the Docker image: (from the cdap/ root directory)::

    docker build -t caskdata/cdap-sandbox cdap-distributions/src

- Run a Docker container from the new image::

    docker run -d -p 11011:11011 -p 11015:11015 caskdata/cdap-sandbox

You now have a new Docker container running with CDAP Sandbox.

Build and Run CDAP Sandbox Locally with Maven
=============================================

The following builds and runs the CDAP sandbox via mvn package. For more details on development
environments and build options, please see the
`BUILD.rst <https://github.com/caskdata/cdap/blob/develop/BUILD.rst>`__ file.

- Obtain a fresh copy of the CDAP (GitHub) repo::

    git clone git@github.com:cdapio/cdap.git

- Build CDAP Sandbox distribution ZIP via mvn::

    MAVEN_OPTS="-Xmx2048m" mvn clean package \
    -pl cdap-standalone,cdap-app-templates/cdap-etl,cdap-app-templates/cdap-program-report \
    -am -amd -DskipTests -P templates,dist,release,unit-tests

- Navigate to cdap-standalone subdirectory, unzip the SDK snapshot, and use the ``cdap`` binary to start the sandbox::

    cd cdap-standalone/target

    unzip cdap-sandbox-<version>-SNAPSHOT.zip && cd cdap-sandbox-<version>-SNAPSHOT/bin

    ./cdap sandbox start

The UI runs on http://localhost:11011. To stop the sandbox, use::

    ./cdap sandbox stop

Note, to include additional artifacts in the CDAP sandbox, such as
`Hydrator plugins <https://github.com/cdapio/hydrator-plugins>`__, include the additional
``-Dadditional.artifacts.dir`` flag in the build step. That is::

    MAVEN_OPTS="-Xmx2048m" mvn clean package \
    -pl cdap-standalone,cdap-app-templates/cdap-etl,cdap-app-templates/cdap-program-report \
    -am -amd -DskipTests -P templates,dist,release,unit-tests
    -Dadditional.artifacts.dir=</path/to/additional/artifacts>
