#!/usr/bin/env bash
#

set -euo pipefail

python scripts/coloc_wrapper.py \
  --sumstat_left "../genetics-finemapping/input/gwas/NEALEUKB_50" \
  --study_left "NEALEUKB_50" \
  --cell_left "" \
  --group_left "" \
  --trait_left "UKB_50" \
  --sumstat_right "../genetics-finemapping/input/molecular_qtl/GTEX7" \
  --study_right "GTEX7" \
  --cell_right "UBERON_0000178" \
  --group_right "ENSG00000186715" \
  --trait_right "eqtl" \
  --chrom "1" \
  --pos "17306029" \
  --method "conditional" \
  --ld_left "../genetics-finemapping/input/ld/EUR.{chrom}.1000Gp3.20130502" \
  --ld_right "../genetics-finemapping/input/ld/EUR.{chrom}.1000Gp3.20130502" \
  --window_coloc "500" \
  --window_cond "2000"

echo COMPLETE
