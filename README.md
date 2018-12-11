Colocalisation pipeline
=======================

Work in progress. Preliminary scripts are [here](https://github.com/edm1/coloc_interim).

### Requirements
- [conda](https://conda.io/docs/)

### Setup environment

```
git clone https://github.com/opentargets/colocalisation.git
cd colocalisation
conda env create -n coloc --file environment.yaml
```

### Edit config files

todo

### Usage

```
todo
```

### Useful commands

```
# Count finished
find output -name "*.json" | wc -l

# Parse time taken for each run
find logs -name "log_file.txt" -exec grep "Time taken" {} \;


```
