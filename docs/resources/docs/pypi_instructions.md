## 1. Create the distributable
```shell 
python setup.py sdist
```

## 2. Upload to PyPi Test
```shell 
twine upload --repository testpypi --skip-existing  dist/*
```
### 2.a Configurar Credenciales para PyPi y TestPyPi
Establezca su nombre de usuario a __token__
Establezca su contraseña al valor de la ficha, incluido el prefijo pypi-
Por ejemplo, si utiliza Twine para cargar sus proyectos en PyPI, monte su archivo $HOME/.pypirc como se muestra a continuación:

```
[pypi]
  username = __token__
  password = pypi-AgEIcHlwaS5vcmcCJDgyNDJlNTJlLTJlNGMtNGVmMC05OTliLTZjYjVhYmVlYzU5YgACK
[testpypi]
  username = __token__
  password = pypi-AgENdGVzdC5weXBpLm9yZwIkYjQ0ZDRkNmUtYjQ1OC00ZTIxLTgzODktYjU4ZWJmZTNlNjU1AAI
```

## 3. Install from test.pypi
```shell 
pip install -i https://test.pypi.org/simple/ cloud-arrow
```

### 3.a Update from test.pypi
```shell 
pip install -i https://test.pypi.org/simple/ --upgrade cloud-arrow
```

## 4. Upload to PyPi main
```shell 
twine upload --skip-existing  dist/*
```

## 5. Install from PyPi main
```shell 
pip install cloud-arrow
```

### 5.a update from PyPi main
```shell 
pip install --upgrade cloud-arrow
```