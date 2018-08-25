.PHONY: clean all start

# Set the environment variable `PYTHON3` to specify the Python binary used.
PYTHON3?=python3.6

all: env/bin/python

env/bin/python:
	$(PYTHON3) -m venv env
	env/bin/pip install --upgrade pip
	env/bin/pip install --upgrade setuptools
	env/bin/pip install wheel
	env/bin/pip install -r requirements.txt
	env/bin/python -m spacy download en
	env/bin/python setup.py develop

clean:
	rm -rfv bin develop-eggs dist downloads eggs env parts .cache build
	rm -fv .DS_Store .coverage .installed.cfg bootstrap.py .coverage
	find . -name '*.pyc' -exec rm -fv {} \;
	find . -name '*.pyo' -exec rm -fv {} \;
	find . -depth -name '*.egg-info' -exec rm -rfv {} \;
	find . -depth -name '__pycache__' -exec rm -rfv {} \;\

start: env/bin/python
	env/bin/start_master --max_grace_seconds=0 --logfile=-
