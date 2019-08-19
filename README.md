# hecrasio

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

---

# Description

**hecrasio** is a collection of tools to read results from [HEC-RAS](https://www.hec.usace.army.mil/software/hec-ras/).

## Contents

##### Hecrasio
- __hecrasio__: Codebase with _core_ and _qaqc_ modules.

##### Notebooks:
- [__QAQC-FluvialTest__](./notebooks/QAQC-FluvialTest.ipynb): Identifies global issues in fluvial models (e.g. model inputs and results for each domain) that require follow-up responses from the modeler.
- [__QAQC-PluvialTest__](./notebooks/QAQC-PluvialTest.ipynb): Identifies global issues in pluvial models (e.g. model inputs and results for each domain) that require follow-up responses from the modeler.

## Launch
To create a virtual environment using [Anaconda](https://www.anaconda.com/distribution/)
```
1. Clone the respository
2. Copy and execute: conda install --yes --file requirements.txt
3. Copy and execute: while read requirement; do conda install --yes $requirement; done < requirements.txt
4. Note, the above will not install boto3. Do so by copying and executing: conda install -c conda-forge boto3=1.9.129
5. Copy and execute: conda install -c conda-forge awscli
6. Configure aws cli by executing: aws congigure
7. Copy and execute: conda install notebook ipykernel
8. Copy and execute: ipython kernelspec install-self
```

## Workflow
_To be added_

## Contributing
Bug reports and feature requests can be submitted through the [Issues](https://github.com/Dewberry/hecrasio/issues/new/choose) tab. When submitting an issue, please follow the provided template.
