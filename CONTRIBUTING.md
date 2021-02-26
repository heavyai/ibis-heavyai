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
    $ conda env create -n ibis-omniscidb-dev --file environment-dev.yml
    $ python -m pip install -e .
    # install git pre-commit hooks
    $ pre-commit install
```

or

```sh
    $ conda create -n ibis-omniscidb-dev python=3.7 pip
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
3. The pull request should work for Python 3.8 or newer.

### Tips

To run a subset of tests:
```sh
    $ pytest tests.test_expr.py
```
### Releasing

For releasing, create a tag with the last commit from master and push it to the repository. It is recommended to use a clone directly from `omnisci` github organization. For example:

```sh
# outside of ibis-omnisci
$ mkdir -p releases
$ cd releases
# if you don't have ssh configured for your github account
# use https://github.com/omnisci/ibis-omniscidb.git
$ git clone git@github.com:omnisci/ibis-omniscidb.git
$ cd ibis-omniscidb
```

If you have already your `ibis-omniscidb` from `omnisci` github
organization, just update your `master` branch:

```sh
git fetch --all
git checkout master
git pull --rebase
```

Create (or re-create) the `conda` environment for the releasing:

```sh
conda env create -n ibis-omniscidb-release --file environment-release.yaml --force
```

And, activate your environment:

```sh
conda activate ibis-omniscidb-release
```

Check the rever configuration:

```sh
rever check
```

Great! We are almost there! Now, visit the WEB page `https://github.com/omnisci/ibis-omniscidb/tags`
and check the latest tag there. For example, if the latest version there is 0.1.0, maybe we want to create a version 0.1.1 or 0.2.0. In this example, we will create a version 0.1.1.

```sh
rever 0.1.1
```

And push the changes to github:

```sh
git push
```

Now, you should be able to see the new tag at `https://github.com/omnisci/ibis-omniscidb/tags`.
