# hecrasio

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

---

# Description

**hecrasio** is a collection of tools to read results from [HEC-RAS](https://www.hec.usace.army.mil/software/hec-ras/) providing quality assurance and control (QA/QC) of one or more notebooks.

## Contents

##### Hecrasio
- __hecrasio__: Codebase with _core_, _qaqc_, _s3tools_, and _heatmap_ modules.

##### Notebooks:
- [__QAQC-PFRA__](./QAQC-PFRA.ipynb): Provides QA/QC of an individual model.
- [__Summary-QAQC__](./Summary-QAQC.ipynb): Summarizes the QA/QC report returned by `QAQC-PFRA` for one or more notebooks.
- [__Make-Heatmap__](./Make-Heatmap.ipynb): Creates heat maps from a weights file and a a set of WSE tifs on s3. Utilizes dask for local parallel processing.

#### Python Files
- `run_postprocess_jobs`: _to be included_
- `PostProcessor`: _to be included_

##### Command File
- `runall`: Executes `PostProcessor` on a range of PFRA results.

## Launch
To create a virtual environment using [Anaconda](https://www.anaconda.com/distribution/)
```
1. Clone the respository
2. Copy and execute: conda install --yes --file requirements.txt
3. Copy and execute: while read requirement; do conda install --yes $requirement; done < requirements.txt
4. Note, the above will not install boto3. Do so by copying and executing: conda install -c conda-forge boto3=1.9.129
5. Copy and execute: conda install -c conda-forge awscli
6. Configure aws cli by executing: aws configure
7. Copy and execute: conda install notebook ipykernel
8. Copy and execute: ipython kernelspec install-self
```

## Workflow
_To be added_

## Contributing
Bug reports and feature requests can be submitted through the [Issues](https://github.com/Dewberry/hecrasio/issues/new/choose) tab. When submitting an issue, please follow the provided template.
