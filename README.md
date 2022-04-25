# Ibis HeavyDB backend

[Ibis](https://ibis-project.org/) is a Python framework to write data analytics
code, in a similar way to [pandas](https://pandas.pydata.org/). Ibis can
execute the queries not only in memory (as pandas does), but in different
backends, including SQL databases, and analytics databases, like
[HeavyDB](https://heavy.ai).


[HeavyDB](https://heavy.ai) is SQL-based,
relational, columnar and specifically developed to harness the massive
parallelism of modern CPU and GPU hardware. HeavyDB can query up to billions
of rows in milliseconds, and is capable of unprecedented ingestion speeds,
making it the ideal SQL engine for the era of big, high-velocity data.

## Install

You can install `ibis-heavyai` via [pip](https://pypi.org/) or
[conda](https://conda.io/):

Install with pip:

```sh
pip install ibis-heavyai
```

Install with conda:

```sh
conda install -c conda-forge ibis-heavyai
```

## Documentation

The ibis-heavyai documentation can be found
[here](https://github.com/heavyai/ibis-heavyai/docs/heavyai.rst).
