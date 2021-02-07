from setuptools import setup


with open('requirements.txt') as f:
    required = f.read().splitlines()

setup(
    name="odx",
    version="1.0.0",
    packages=["odx"],
    install_requires=required,
)
