# AWS Lambda Layer: pandas + faker

Commands used to build a Linux-compatible AWS Lambda layer that bundles the `pandas` and `faker` Python packages.

## 1. Build Linux-compatible dependencies for the Lambda layer (x86_64)

```powershell
pip install pandas faker `
  --platform manylinux2014_x86_64 `
  --only-binary=:all: `
  --target python `
  --python-version 3.12
```

This creates a `python` directory with the dependencies laid out in the structure expected by AWS Lambda layers.

## 2. Zip the Lambda layer contents into an archive

```powershell
Compress-Archive -Path python -DestinationPath pandas_faker_layer.zip
```
