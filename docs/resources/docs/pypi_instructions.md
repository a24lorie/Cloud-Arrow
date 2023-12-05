1. create the distributable
```shell 
python setup.py sdist
```

2. upload to PyPi Test
```shell 
twine upload --repository testpypi --skip-existing  dist/*
```

3. install from testpypi
```shell 
pip install -i https://test.pypi.org/simple/ cloud-arrow
```

3.a update from testpypi
```shell 
pip install -i https://test.pypi.org/simple/ --upgrade cloud-arrow
```

4. upload to PyPi main
```shell 
twine upload --skip-existing  dist/*
```

5. install from PyPi main
```shell 
pip install cloud-arrow
```

5.a update from PyPi main
```shell 
pip install --upgrade cloud-arrow
```