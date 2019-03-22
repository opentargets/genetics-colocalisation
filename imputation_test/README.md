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


### Notes

### Low overlap using R2 > 0.7

When calculating LD using R-squared > 0.7, there is little overlap between the coloc and LD datasets:

Rows with LD only:  377
- Not sure why this occurs
Rows with coloc only:  25292
- Occurs due to lack of LD between lead variants
Rows with LD and coloc:  1010

## When changing to R2 > 0.05

Rows with LD only:  2597
Rows with coloc only:  19723
Rows with LD and coloc:  6584