test:
	PYTHONWARNINGS=default pytest --cov=greenstalk tests.py

lint:
	flake8 greenstalk.py tests.py
	isort greenstalk.py tests.py --multi-line 5
	mypy greenstalk.py tests.py --ignore-missing-imports --strict-optional --disallow-untyped-defs --no-strict-optional

clean:
	rm -rf .cache/ .coverage *.egg-info/ __pycache__/ .tox/ .mypy_cache/ docs/_build/
