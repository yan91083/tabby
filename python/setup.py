from setuptools import find_packages, setup

setup(
    name="tabby",
    packages=find_packages(exclude=["tabby_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "dagster-pandas"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
