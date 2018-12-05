TGraph Database
----------------
> TGraph is a database system designed for managing temporal graph data.
>
> [TGraph database](http://dx.doi.org/10.1145/2983323.2983335) is build on top of Neo4j.


Watch [this video](http://amitabha.water-crystal.org/TGraphDemo.mp4) to know the power of TGraph database via a [demonstration](https://github.com/TGraphDB/gephi-demo/tree/TGraphDemo/modules/TGraphDemo) (a tiny traffic-query-system on a specific data set) in 3 minutes. The demo GUI is implemented as a Gephi plugin.

## System requirement for TGraph
- Java Runtime Environment version 8.0 or higher

# Installation
TGraph currently is an [extended Neo4j](https://github.com/TGraphDB/temporal-neo4j) which integrates a [temporal property storage system](https://github.com/TGraphDB/temporal-storage) (TPS).

First build the temporal property storage (TPS) system. Clone the code of TPS, then run `mvn -B install -Dmaven.test.skip=true` in TPS's dir.

Second build the extended Neo4j. Clone the code, then run `mvn -B install -pl org.neo4j:neo4j-lucene-index -DskipTests -Dlicensing.skip -Dlicense.skip -am` in dir.

# Notes

3. Gephi may produce an `Run out of memory` notice to you when you `import` the road network to ask you enlarge the memory which Gephi could use in your computer. This is because the default max size of memory which Gephi can use is 512MB. You must have at least 2GB free memory for Gephi to play with an existing database. The more memory you give, the faster the program runs.

6. To build this plugin from source code, please wait for our release of `TGraph-kernel` and `tgraph-temporal-storage` maven package.

# Cite TGraph Database Management System
Please cite our [demo paper](http://dx.doi.org/10.1145/2983323.2983335) published in CIKM'2016.