from setuptools import find_packages, setup

dependences = [
    "environs",
    "requests",
    "click",
]

setup(
    name="epaaqs",
    packages=find_packages(exclude=["tests"]),
    version="0.1.0",
    description="API to retrieve Air Quality datasets from the EPA AQS",
    author="Alex Biehl",
    license="BSD",
    install_requires=dependences,
    include_package_data=True,
    entry_points={"console_scripts": ["epa-aqs = epa_aqs_api.cli:main"]},
    tests_require=["pytest", "coverage"],
    test_suite="tests",
)
