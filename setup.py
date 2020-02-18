from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()

with open("requirements.txt", "r") as fh:
    requirements = fh.readlines()

setup(
    name="acta",
    version="0.4.1",
    long_description=long_description,
    long_description_content_type="text/markdown",
    description="an actor framework on top of asyncio with command/event pattern",
    url="https://github.com/ybrs/acta",
    packages=["acta"],
    zip_safe=True,
    install_requires=requirements,
    python_requires=">3.7.2",
    test_require=["psutil>=5.2.2"],
    entry_points={"console_scripts": ["acta-world = acta.subsystem:run_world"]},
)
