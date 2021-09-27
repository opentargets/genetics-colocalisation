suppressMessages(library(tidyverse))
#library(rjson)
library(jsonlite)

# The purpose of this script was to figure out why, when I filter the manifest
# of coloc tests to do, I was having more than expected.
# There were 2.4 M overlaps with the new (full) dataset, and previously we had
# 1.7 M coloc tests done.  But after filtering these out (or trying to), I
# still found 1.1 M tests to do, wne it should be less than 0.7 M.

# The analyses below helped me to figure out that it's because I wasn't matching
# fully on the phenotype ID and biofeature columns. Some of these columns have
# a null value - e.g. the key isn't found in the json line in the overlap table.
# But some have the text "None", I believe. This can cause matches to fail.
colocs = read_tsv("/Users/jeremys/Downloads/coloc_raw.tsv.gz")

overlaps = jsonlite::stream_in(file("/Users/jeremys/Downloads/overlap_table.json.gz")) %>%
  rename(left_study = left_study_id,
         left_chrom = left_lead_chrom ,
         left_ref = left_lead_ref,
         left_alt = left_lead_alt,
         left_phenotype = left_phenotype_id,
         right_study = right_study_id,
         right_chrom = right_lead_chrom,
         right_ref = right_lead_ref,
         right_alt = right_lead_alt,
         right_phenotype = right_phenotype_id) %>%
  mutate(left_chrom = as.character(left_chrom),
         right_chrom = as.character(right_chrom))

overlaps = overlaps %>%
  rename(left_study = left_study_id,
         left_chrom = left_lead_chrom ,
         left_pos = left_lead_pos ,
         left_ref = left_lead_ref,
         left_alt = left_lead_alt,
         left_phenotype = left_phenotype_id,
         right_study = right_study_id,
         right_chrom = right_lead_chrom,
         right_pos = right_lead_pos,
         right_ref = right_lead_ref,
         right_alt = right_lead_alt,
         right_phenotype = right_phenotype_id)

colocs = colocs %>%
  mutate(left_chrom = as.character(left_chrom),
         right_chrom = as.character(right_chrom))

colnames(colocs)
colnames(overlaps)

# Find how many previous coloc tests are found in our current overlaps
joined = colocs %>%
  inner_join(overlaps, by=c("left_study", "left_chrom", "left_pos", "left_ref", "left_alt", "left_phenotype", "left_bio_feature", "right_study", "right_chrom", "right_pos", "right_ref", "right_alt", "right_phenotype", "right_bio_feature"))
nrow(joined)
# Basically all are found, so this is fine.

# Find how many overlaps should be considered "new"
newtests = overlaps %>%
  anti_join(colocs, by=c("left_study", "left_chrom", "left_pos", "left_ref", "left_alt", "left_phenotype", "left_bio_feature", "right_study", "right_chrom", "right_pos", "right_ref", "right_alt", "right_phenotype", "right_bio_feature"))
nrow(newtests)
# This gives us the expected number. So it's working in R, though it
# didn't work the way I expected in the python code to filter the manifest.

manifest = jsonlite::stream_in(file("/Users/jeremys/Downloads/manifest.json.gz"))
  rename(left_study = left_study_id,
         left_chrom = left_lead_chrom ,
         left_pos = left_lead_pos ,
         left_ref = left_lead_ref,
         left_alt = left_lead_alt,
         left_phenotype = left_phenotype_id,
         right_study = right_study_id,
         right_chrom = right_lead_chrom,
         right_pos = right_lead_pos,
         right_ref = right_lead_ref,
         right_alt = right_lead_alt,
         right_phenotype = right_phenotype_id)

# Ensure that empty phenotypes and biofeatures are NA rather than the text "None"
manifest2 = manifest
manifest2$left_phenotype[manifest2$left_phenotype == "None"] = NA
manifest2$left_bio_feature[manifest2$left_bio_feature == "None"] = NA
manifest2$right_phenotype[manifest2$right_phenotype == "None"] = NA
manifest2$right_bio_feature[manifest2$right_bio_feature == "None"] = NA

extra = manifest2 %>%
  inner_join(colocs, by=c("left_study", "left_chrom", "left_pos", "left_ref", "left_alt", "left_phenotype", "left_bio_feature", "right_study", "right_chrom", "right_pos", "right_ref", "right_alt", "right_phenotype", "right_bio_feature"))

# Use the fixed manifest to look for what new coloc tests should be done
newtests2 = manifest2 %>%
  anti_join(colocs, by=c("left_study", "left_chrom", "left_pos", "left_ref", "left_alt", "left_phenotype", "left_bio_feature", "right_study", "right_chrom", "right_pos", "right_ref", "right_alt", "right_phenotype", "right_bio_feature"))
# This gives just slightly fewer (660,000 vs. 672,000) than before, which is due
# to correctly matching tests with "None" phenotype/biofeatures that were done before.

