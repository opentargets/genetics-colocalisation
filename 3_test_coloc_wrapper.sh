#!/usr/bin/env bash
#

set -euo pipefail

# {
#     "left_type": "gwas",
#     "left_study_id": "GCST004132_cr",
#     "left_lead_chrom": "22",
#     "left_lead_pos": 39264824,
#     "left_lead_ref": "T",
#     "left_lead_alt": "C",
#     "right_type": "eqtl",
#     "right_study_id": "CEDAR",
#     "right_phenotype_id": "ILMN_1739086",
#     "right_biofeature": "MONOCYTE_CD14",
#     "right_lead_chrom": "22",
#     "right_lead_pos": 38344863,
#     "right_lead_ref": "AC",
#     "right_lead_alt": "A",
#     "num_overlapping": 4,
#     "left_num_tags": 4,
#     "right_num_tags": 4604,
#     "min_num_tags": 4,
#     "proportion_overlap": 1.0
# }

# python scripts/coloc_wrapper.py \
#   --left_sumstat "../genetics-finemapping/example_data/sumstats/gwas/GCST004132_cr.parquet" \
#   --left_type "gwas" \
#   --left_study "GCST004132_cr" \
#   --left_phenotype "None" \
#   --left_biofeature "None" \
#   --left_chrom "22" \
#   --left_pos "39264824" \
#   --left_ref "T" \
#   --left_alt "C" \
#   --right_sumstat "../genetics-finemapping/example_data/sumstats/molecular_trait/CEDAR.parquet" \
#   --right_type "molecular_trait" \
#   --right_study "CEDAR" \
#   --right_phenotype "ILMN_1739086" \
#   --right_biofeature "MONOCYTE_CD14" \
#   --right_chrom "22" \
#   --right_pos "38344863" \
#   --right_ref "AC" \
#   --right_alt "A" \
#   --r_coloc_script "scripts/coloc.R" \
#   --method "conditional" \
#   --top_loci "../genetics-finemapping/output/top_loci.json.gz" \
#   --left_ld "/Users/em21/Projects/reference_data/uk10k_2019Feb/3_liftover_to_GRCh38/output/22.ALSPAC_TWINSUK.maf01.beagle.csq.shapeit.20131101" \
#   --right_ld "/Users/em21/Projects/reference_data/uk10k_2019Feb/3_liftover_to_GRCh38/output/22.ALSPAC_TWINSUK.maf01.beagle.csq.shapeit.20131101" \
#   --window_coloc "500" \
#   --window_cond "2000" \
#   --min_maf "0.01" \
#   --out "output/left_study=GCST004132_cr/left_phenotype=/left_biofeature=/left_variant=22_39264824_T_C/right_study=CEDAR/right_phenotype=ILMN_1739086/right_biofeature=MONOCYTE_CD14/right_variant=22_38344863_AC_A/coloc_res.json" \
#   --plot "plots/GCST004132_cr__22_39264824_T_C_CEDAR_ILMN_1739086_MONOCYTE_CD14_22_38344863_AC_A.plot.png" \
#   --log "logs/left_study=GCST004132_cr/left_phenotype=/left_biofeature=/left_variant=22_39264824_T_C/right_study=CEDAR/right_phenotype=ILMN_1739086/right_biofeature=MONOCYTE_CD14/right_variant=22_38344863_AC_A/log_file.txt" \
#   --tmpdir "tmp/left_study=GCST004132_cr/left_phenotype=/left_biofeature=/left_variant=22_39264824_T_C/right_study=CEDAR/right_phenotype=ILMN_1739086/right_biofeature=MONOCYTE_CD14/right_variant=22_38344863_AC_A/"


# {
#     "left_type": "gwas",
#     "left_study_id": "GCST004132_cr",
#     "left_lead_chrom": "22",
#     "left_lead_pos": 21590807,
#     "left_lead_ref": "T",
#     "left_lead_alt": "A",
#     "right_type": "eqtl",
#     "right_study_id": "CEDAR",
#     "right_phenotype_id": "ILMN_1677877",
#     "right_biofeature": "MONOCYTE_CD14",
#     "right_lead_chrom": "22",
#     "right_lead_pos": 21625295,
#     "right_lead_ref": "C",
#     "right_lead_alt": "T",
#     "num_overlapping": 54,
#     "left_num_tags": 57,
#     "right_num_tags": 90,
#     "min_num_tags": 57,
#     "proportion_overlap": 0.9473684210526315
# }

python scripts/coloc_wrapper.py \
  --left_sumstat "../genetics-finemapping/example_data/sumstats/gwas/GCST004132_cr.parquet" \
  --left_type "gwas" \
  --left_study "GCST004132_cr" \
  --left_phenotype "None" \
  --left_biofeature "None" \
  --left_chrom "22" \
  --left_pos "21590807" \
  --left_ref "T" \
  --left_alt "A" \
  --right_sumstat "../genetics-finemapping/example_data/sumstats/molecular_trait/CEDAR.parquet" \
  --right_type "molecular_trait" \
  --right_study "CEDAR" \
  --right_phenotype "ILMN_1677877" \
  --right_biofeature "MONOCYTE_CD14" \
  --right_chrom "22" \
  --right_pos "21625295" \
  --right_ref "C" \
  --right_alt "T" \
  --r_coloc_script "scripts/coloc.R" \
  --method "conditional" \
  --top_loci "../genetics-finemapping/output/top_loci.json.gz" \
  --left_ld "/Users/em21/Projects/reference_data/uk10k_2019Feb/3_liftover_to_GRCh38/output/22.ALSPAC_TWINSUK.maf01.beagle.csq.shapeit.20131101" \
  --right_ld "/Users/em21/Projects/reference_data/uk10k_2019Feb/3_liftover_to_GRCh38/output/22.ALSPAC_TWINSUK.maf01.beagle.csq.shapeit.20131101" \
  --window_coloc "500" \
  --window_cond "2000" \
  --min_maf "0.01" \
  --out "output/left_study=GCST004132_cr/left_phenotype=/left_biofeature=/left_variant=22_21590807_T_A/right_study=CEDAR/right_phenotype=ILMN_1677877/right_biofeature=MONOCYTE_CD14/right_variant=22_21625295_C_T/coloc_res.json" \
  --plot "plots/GCST004132_cr__22_21590807_T_A_CEDAR_ILMN_1677877_MONOCYTE_CD14_22_21625295_C_T.plot.png" \
  --log "logs/left_study=GCST004132_cr/left_phenotype=/left_biofeature=/left_variant=22_21590807_T_A/right_study=CEDAR/right_phenotype=ILMN_1677877/right_biofeature=MONOCYTE_CD14/right_variant=22_21625295_C_T/log_file.txt" \
  --tmpdir "tmp/left_study=GCST004132_cr/left_phenotype=/left_biofeature=/left_variant=22_21590807_T_A/right_study=CEDAR/right_phenotype=ILMN_1677877/right_biofeature=MONOCYTE_CD14/right_variant=22_21625295_C_T/"

echo COMPLETE
