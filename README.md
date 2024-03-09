# Isolated  Pipelines with Dagster and lakeFS

## Getting Started

Clone this repository and checkout the `before` branch from this repository. 
This will reflect a non-sandboxed pipeline written in Python that is already using Dagster.

The data will be stored on S3 and the code will live inside this repository.

```shell
git checkout before
```

Now, let's make sure we have the necessary requirements installed:

```shell
# optionally, consider creating a vritualenv or similar to install these dependencies into
pip install -r requirements.txt
```

Now that the requirements are installed, let's run our pipeline:

```shell
dagster
```