Coloc imputation
================

Is it possible to impute coloc stats using LD alone?

Proposed predictors/features:
- -log(pval) for A and B lead variant
- A/B distinct and overlapping
- abs(distance between A and B leads)
- for different values of LD-R^2, how many variants overlap?

Todo:
- Comparisons with coloc truths have been pre-selected. Need to investigate how this will affect imputation.
