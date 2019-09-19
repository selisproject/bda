SELIS Big Data Analytics Module
===============================

About
-----
This repository contains the source code of the SELIS Big Data Analytics component.

Installation
------------
Create a _conf/bda.properties_ file using the provided template. Compile and run with:

```
mvn initialize
mvn verify
mvn package

cd bda-controller 
/bin/sh -c ./src/main/scripts/selis-bda-server.sh
```

Contact
-------
selis@cslab.ece.ntua.gr 
