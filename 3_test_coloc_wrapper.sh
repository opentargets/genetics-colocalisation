#!/usr/bin/env bash
#

set -euo pipefail

python scripts/coloc_wrapper.py \
  --sumstat_left "../genetics-finemapping/input/gwas/NEALEUKB_50" \
  --study_left "NEALEUKB_50" \
  --cell_left "" \
  --group_left "" \
  --trait_left "UKB_50" \
  --chrom_left "1" \
  --pos_left "17306029" \
  --ref_left "C" \
  --alt_left "A" \
  --sumstat_right "../genetics-finemapping/input/molecular_qtl/GTEX7" \
  --study_right "GTEX7" \
  --cell_right "UBERON_0000178" \
  --group_right "ENSG00000186715" \
  --trait_right "eqtl" \
  --chrom_right "1" \
  --pos_right "17270751" \
  --ref_right "G" \
  --alt_right "A" \
  --method "conditional" \
  --ld_left "../genetics-finemapping/input/ld/EUR.{chrom}.1000Gp3.20130502" \
  --ld_right "../genetics-finemapping/input/ld/EUR.{chrom}.1000Gp3.20130502" \
  --window_coloc "500" \
  --window_cond "2000" \
  --out "output/study_left=NEALEUKB_50/cell_left=/group_left=/trait_left=UKB_50/variant_left=1_17306029_C_A/study_right=GTEX7/cell_right=UBERON_0000178/group_right=ENSG00000186715/trait_right=UKB_50/variant_right=1_17270751_G_A/coloc_res.tsv" \
  --log "logs/study_left=NEALEUKB_50/cell_left=/group_left=/trait_left=UKB_50/variant_left=1_17306029_C_A/study_right=GTEX7/cell_right=UBERON_0000178/group_right=ENSG00000186715/trait_right=UKB_50/variant_right=1_17270751_G_A/log_file.txt" \
  --tmpdir "tmp/study_left=NEALEUKB_50/cell_left=/group_left=/trait_left=UKB_50/variant_left=1_17306029_C_A/study_right=GTEX7/cell_right=UBERON_0000178/group_right=ENSG00000186715/trait_right=UKB_50/variant_right=1_17270751_G_A/"


echo COMPLETE
