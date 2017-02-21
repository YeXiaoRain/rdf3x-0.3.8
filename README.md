Usage

RDF-3X can import NTriples/Turtle RDF data. As an example we use a Turtle dump of [Yago](http://www.mpi-inf.mpg.de/~suchanek/downloads/yago/). Download RDF-3X, build it, and download and extract the Yago dump (236 MB). Then build a new database using:

`rdf3xload db yago.n3`

This takes ca. 30 minutes on a laptop with 2GB main memory. Afterwards start the query interface using:

`rdf3xquery db`

This query interface accepts standard SPARQL queries, for example:

`select ?name where { ?p <isCalled> ?name. ?p <bornInLocation> <London> }`
