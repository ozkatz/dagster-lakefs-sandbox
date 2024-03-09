from setuptools import find_packages, setup

setup(
    name="taxi_pipelines",
    packages=find_packages(exclude=["taxi_pipelines_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
