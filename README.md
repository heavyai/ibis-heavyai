# Ibis OmniSciDB backend

[Ibis](https://ibis-project.org/) is a Python framework to write data analytics
code, in a similar way to [pandas](https://pandas.pydata.org/). Ibis can
execute the queries not only in memory (as pandas does), but in different
backends, including SQL databases, and analytics databases, like
[OmniSciDB](https://www.omnisci.com/platform/omniscidb).


[OmniSciDB](https://www.omnisci.com/platform/omniscidb) is SQL-based,
relational, columnar and specifically developed to harness the massive
parallelism of modern CPU and GPU hardware. OmniSciDB can query up to billions
of rows in milliseconds, and is capable of unprecedented ingestion speeds,
making it the ideal SQL engine for the era of big, high-velocity data.

## Install

You can install `ibis-omniscidb` via [pip](https://pypi.org/) or
[conda](https://conda.io/):

Install with pip:

```sh
pip install ibis-omniscidb
```

Install with conda:

```sh
conda install -c conda-forge ibis-omniscidb
```

## Documentation

The [ibis-omniscidb](https://ibis-project.org/docs/backends/omnisci.html)
documentation can be found in the [Ibis website](https://ibis-project.org/).
