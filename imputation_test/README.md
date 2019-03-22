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

# Results of imputation (22nd March 2019)

OLS h3 all; R^2=0.18 ± 0.02
OLS h4 all; R^2=0.71 ± 0.01
OLS log_h4_h3 all; R^2=0.49 ± 0.06
RandomForest_100 h3 all; R^2=0.54 ± 0.02
RandomForest_100 h4 all; R^2=0.87 ± 0.01
RandomForest_100 log_h4_h3 all; R^2=0.77 ± 0.01
OLS h3 gwas; R^2=-0.16 ± 0.41
OLS h4 gwas; R^2=-0.11 ± 0.28
OLS log_h4_h3 gwas; R^2=0.06 ± 0.20
RandomForest_100 h3 gwas; R^2=0.47 ± 0.12
RandomForest_100 h4 gwas; R^2=0.43 ± 0.13
RandomForest_100 log_h4_h3 gwas; R^2=0.54 ± 0.22
OLS h3 molecular_trait; R^2=0.13 ± 0.02
OLS h4 molecular_trait; R^2=0.61 ± 0.02
OLS log_h4_h3 molecular_trait; R^2=0.33 ± 0.03
RandomForest_100 h3 molecular_trait; R^2=0.50 ± 0.01
RandomForest_100 h4 molecular_trait; R^2=0.81 ± 0.02
RandomForest_100 log_h4_h3 molecular_trait; R^2=0.62 ± 0.02