from setuptools import find_packages, setup

setup(
    name="epa-aqs",
    packages=find_packages("epa_aqs_api"),
    version="0.1.0",
    description="API to retrieve Air Quality datasets from the EPA AQS",
    author="Alex Biehl",
    license="MIT",
    setup_requires=[
        "environs",
        "requests",
        "click",
    ],
    entry_points={
      "console_scripts":[
          "epa-aqs=epa_aqs_api.app:cli"
      ]
    },
    tests_require=[
        "pytest",
        "coverage"
    ],
    test_suite="tests",
)
