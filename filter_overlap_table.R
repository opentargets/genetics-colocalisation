library(tidyverse)
library(jsonlite)

#top_loci = read_csv('/Users/jeremys/work/otgenetics/genetics-analysis/finemapping/data/210923/top_loci.csv.gz', guess_max = 1e7)
top_loci = jsonlite::stream_in(file('/Users/jeremys/work/otgenetics/genetics-colocalisation/top_loci.json.gz'))

sum(top_loci$pval == 0)
sum(top_loci$pval < 1e-300)


overlap_table_in = jsonlite::stream_in(file("/Users/jeremys/work/otgenetics/genetics-colocalisation/overlap_table_in.json.gz"))

overlap_table = overlap_table_in %>%
  mutate(left_variant_id = paste(left_lead_chrom, left_lead_pos, left_lead_ref, left_lead_alt, sep=':')) %>%
  mutate(right_variant_id = paste(right_lead_chrom, right_lead_pos, right_lead_ref, right_lead_alt, sep=':'))

overlap_table = overlap_table %>%
  left_join(top_loci %>% select(study_id, variant_id, left_pval = pval),
            by=c("left_study_id"="study_id", "left_variant_id"="variant_id")) %>%
  left_join(top_loci %>% select(study_id, variant_id, phenotype_id, bio_feature, right_pval = pval),
            by=c("right_study_id"="study_id", "right_phenotype_id"="phenotype_id", "right_bio_feature"="bio_feature", "right_variant_id"="variant_id"))

overlap_low_p = overlap_table %>%
  filter(left_pval < 1e-300 | right_pval < 1e-300) %>%
  select(-left_variant_id, -right_variant_id, -left_pval, -right_pval)

jsonlite::stream_out(overlap_low_p, gzfile("/output/overlap_table.json.gz"))
