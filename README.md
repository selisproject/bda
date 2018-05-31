SELIS Big Data Analytics Module
===============================

About
-----
This repository contains the source code of the SELIS Big Data Analytics component.

Installation
------------
Edit _bda.properties_ file. Compile and run with:

```
mvn initialize
mvn verify
mvn package -DskipTests

cd bda-controller 
/bin/sh -c ./src/main/scripts/selis-bda-server.sh
```

License
-------
TBD

Contact
-------
selis@cslab.ece.ntua.gr 
