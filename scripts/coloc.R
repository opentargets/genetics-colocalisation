#!/usr/bin/env Rscript
#

library("tidyverse")
library("coloc")

# Options
if (interactive()) {
  setwd('/Users/em21/Projects/genetics-colocalisation')
  in_left_ss = "tmp/coloc_180908/NEALEUKB_23112/22_18215952_G_C/gtex_v7/UBERON_0004264/ENSG00000015475/left_ss.tsv.gz"
  in_right_ss = "tmp/coloc_180908/NEALEUKB_23112/22_18215952_G_C/gtex_v7/UBERON_0004264/ENSG00000015475/right_ss.tsv.gz"
  outpref = "tmp/coloc_180908/NEALEUKB_23112/22_18215952_G_C/gtex_v7/UBERON_0004264/ENSG00000015475/coloc"
} else {
  args = commandArgs(trailingOnly=TRUE)
  in_left_ss = args[1]
  in_right_ss = args[2]
  outpref = args[3]
}

# Load data
left_ss = read.table(in_left_ss, sep="\t", header=T)
left_ss$side = 'left'
right_ss = read.table(in_right_ss, sep="\t", header=T)
right_ss$side = 'right'

# EAF to MAF function
eaf_to_maf = function(eaf) {
  min(eaf, 1-eaf)
}

# Make coloc dataset (left)
left_n = left_ss[1, 'n_total']
left_ncases = left_ss[1, 'n_cases']
left_ncases = min(left_ncases, (left_n - left_ncases))
left_prop = left_ncases / left_n
left_type = ifelse(as.character(left_ss[1, 'is_cc']) == 'True', 'cc', 'quant')
left_data = list(
                 pvalues=left_ss$pval,
                 N=left_n,
                 MAF=sapply(left_ss$eaf, eaf_to_maf),
                 type=left_type,
                 s=left_prop
               )

# Make coloc dataset (right)
right_n = right_ss[1, 'n_total']
right_ncases = right_ss[1, 'n_cases']
right_ncases = min(right_ncases, (right_n - right_ncases))
right_prop = right_ncases / right_n
right_type = ifelse(as.character(right_ss[1, 'is_cc']) == 'True', 'cc', 'quant')
right_data = list(
                 pvalues=right_ss$pval,
                 N=right_n,
                 MAF=sapply(right_ss$eaf, eaf_to_maf),
                 type=right_type,
                 s=right_prop
               )


# # Make coloc dataset (right). Use left_ss's maf if right has no maf
# right_n = right_ss[1, 'n_samples'] #* 10
# right_type = ifelse(as.character(right_ss[1, 'is_cc']) == 'True', 'cc', 'quant')
# print(right_type)
# right_data = list(
#                  pvalues=right_ss$pval,
#                  N=right_n,
#                  MAF=min(right_ss$maf, 1-right_ss$maf),
#                  type=right_type
#                 )

# Run coloc
coloc_res = coloc.abf(left_data, right_data,
                     MAF=left_ss$maf,
                     p1=1e-04, p2=1e-04, p12 = 1e-06)

# Output coloc result
coloc_summ = data.frame(coloc_res$summary)
coloc_summ$field = rownames(coloc_summ)
colnames(coloc_summ) = c('value', 'field')
out_f = paste0(outpref, '.pp.tsv')
write_tsv(coloc_summ[,c('field', 'value')], out_f, col_names=T)

# Output full results
# coloc_full = data.frame(coloc_res$results)
# out_f = paste0(outpref, '.full.tsv.gz')
# write_tsv(coloc_full, out_f, col_names=T)

# Plot
# plot_data = rbind(left_ss[, c('pos_b37', 'pval', 'side')],
#                   right_ss[, c('pos_b37', 'pval', 'side')])
# p = ggplot(plot_data, aes(x=pos_b37, y=-log10(pval), colour=side)) + geom_point(alpha=0.5, size=1)
# out_f = paste0(outpref, '.plot.png')
# ggsave(p, file=out_f, w=12, h=8, units='cm', dpi=150)
