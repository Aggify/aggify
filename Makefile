build:
	python setup.py sdist

# Publish to PyPI using twine
publish:
	twine check dist/*
	twine upload dist/*
