# Pinned to a Python 3.11 miniconda: the pinned pytest 3.6.1 imports the `imp`
# module, which was removed in Python 3.12. `:latest` now ships Python 3.14 and
# breaks test collection.
FROM continuumio/miniconda3:23.10.0-1

COPY requirements.txt /
RUN pip install -r /requirements.txt

WORKDIR /project
