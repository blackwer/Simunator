from setuptools import setup


setup(
    name="simunator",
    version="0.0.1",
    description="Package for iteratively running, analyzing, maintaining, and aggregating simulation data",
    url="https://github.com/blackwer/Simunator",
    author="Robert Blackwell",
    author_email="rblackwell@flatironinstitute.org",
    license="Apache 2.0",
    license_files=('LICENSE'),
    packages=["simunator"],
    install_requires=["numpy", "jinja2", "doltpy"],
    scripts=['bin/simunator'],
)
