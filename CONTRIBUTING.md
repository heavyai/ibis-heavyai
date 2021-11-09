# Contributing Guide


Contributions are welcome, and they are greatly appreciated! Every little bit
helps, and credit will always be given.

You can contribute in many ways:

## Types of Contributions

### Report Bugs

Report bugs at https://github.com/omnisci/ibis-omniscidb/issues.

If you are reporting a bug, please include:

* Your operating system name and version.
* Any details about your local setup that might be helpful in troubleshooting.
* Detailed steps to reproduce the bug.

### Fix Bugs

Look through the GitHub issues for bugs. Anything tagged with "bug" and "help
wanted" is open to whoever wants to implement it.

### Implement Features

Look through the GitHub issues for features. Anything tagged with "enhancement"
and "help wanted" is open to whoever wants to implement it.

### Write Documentation

ibis-omniscidb could always use more documentation, whether as part of the
official ibis-omniscidb docs, in docstrings, or even on the web in blog posts,
articles, and such.

### Submit Feedback

The best way to send feedback is to file an issue at https://github.com/omnisci/ibis-omniscidb/issues.

If you are proposing a feature:

* Explain in detail how it would work.
* Keep the scope as narrow as possible, to make it easier to implement.
* Remember that this is a volunteer-driven project, and that contributions
  are welcome :)

### Get Started!

Ready to contribute? Here's how to set up `ibis-omniscidb` for local development.

1. Fork the `ibis-omniscidb` repo on GitHub.
2. Clone your fork locally:
```sh
$ git clone git@github.com:your_name_here/ibis-omniscidb.git
```
3. Install your local copy into a virtual environment. Assuming want to use conda environment,
   this is how you set up your fork for local development:
```sh
    $ conda env create -n ibis-omniscidb --file environment-dev.yml
    $ python -m pip install -e .
    # install git pre-commit hooks
    $ pre-commit install
```

or

```sh
    $ conda create -n ibis-omniscidb python=3.7 pip
    $ python -m pip install -r requirements.txt
    $ python -m pip install -e .
    # install git pre-commit hooks
    $ pre-commit install
```

4. Create a branch for local development:
```sh
    $ git checkout -b name-of-your-bugfix-or-feature
```
5. Commit your changes and push your branch to GitHub. When you commit a change, as it uses git pre-commit, it will run flake8, mypy, black and isort before commit any change:
```sh
    $ git add .
    $ git commit -m "Your detailed description of your changes."
    $ git push origin name-of-your-bugfix-or-feature
```
6. Submit a pull request through the GitHub website.
`https://github.com/omnisci/ibis-omniscidb/tags`
Pull Request Guidelines
-----------------------

Before you submit a pull request, check that it meets these guidelines:

1. The pull request should include tests.
2. If the pull request adds functionality, the docs should be updated. Put your new functionality into a function with a docstring.
3. The pull request should work for Python 3.7 or newer.

### Tips

To run a subset of tests:
```sh
    $ pytest tests.test_expr.py
```
### Releasing

To cut a new release, go 
[GitHub Releases](https://github.com/Quansight/qadmin/releases/new) 
Add the information for the new release and then click on 
"Publish Release", it trigger a CI job that will cut a release at
PyPI. When the package is released at PyPI, it will trigger a new
release at conda-forge.
