#!/usr/bin/env Rscript
#
library("data.table", include.only = 'fread')
library("magrittr", include.only = '%>%')

main = function() {
  args = commandArgs(trailingOnly=TRUE)
  manifest_file = args[1]
  
  start_time = Sys.time()
  print(sprintf("Start time: %s", start_time))
  print(sprintf("Processing manifest file: %s", manifest_file))
  
  manifest = read.table(manifest_file, sep='\t', header=F, stringsAsFactors=F)
  colnames(manifest) = c("left_sumstats", "right_sumstats", "out", "log")
  
  print(sprintf("Running %d coloc tests from manifest", nrow(manifest)))
  
  error_count = 0
  # Rprof()
  
  for (i in 1:nrow(manifest)) {
    # Use sink to redirect all output to the log file for this coloc test
    print(sprintf("Logging to file: %s", manifest$log[i]))
    sink(manifest$log[i])
    
    # Run coloc, and catch any errors and continue
    tryCatch(
      do_coloc(manifest$left_sumstats[i],
               manifest$right_sumstats[i],
               manifest$out[i],
               manifest$log[i]),
      error = function(e) {
        print(e)
        print(e$message)
        print(e$trace)
        error_count = error_count + 1
      }
    )
    
    # Output any warnings from this coloc test
    warnings()
    # Restore sink to normal (important since we're running this in a loop)
    sink()
  }
  
  print(sprintf("Completed processing. Num errors caught: %d, file: %s"), error_count, manifest_file)
  
  end_time <- Sys.time()
  print(sprintf("End time: %s", start_time))
  print(sprintf("Time taken: %s", format(end_time - start_time)))
  time.taken <- end_time - start_time
  
  # Rprof(NULL)
  # print(summaryRprof())
}


do_coloc = function(in_left_ss, in_right_ss, outfile, logfile) {
  print(sprintf("Coloc left sumstat,right sumstat,outfile,logfile\n%s,%s,%s,%s",
                in_left_ss, in_right_ss, outfile, logfile))
  # Make output directory
  dir.create(dirname(outfile), recursive = TRUE, showWarnings = FALSE)
  
  # Load data
  left_ss = fread(in_left_ss) %>%
    dplyr::filter(!is.na(beta))
  
  right_ss = fread(in_right_ss) %>%
    dplyr::filter(!is.na(beta))
  
  # Store some basic values
  left_n = left_ss$n_total[1]
  left_ncases = left_ss$n_cases[1]
  left_ncases = min(left_ncases, (left_n - left_ncases))
  left_prop = left_ncases / left_n
  left_type = ifelse(left_ss$is_cc[1] == TRUE, 'cc', 'quant')
  
  right_n = right_ss$n_total[1]
  right_ncases = right_ss$n_cases[1]
  right_ncases = min(right_ncases, (right_n - right_ncases))
  right_prop = right_ncases / right_n
  right_type = ifelse(right_ss$is_cc[1] == TRUE, 'cc', 'quant')
  
  # Subset to relevant columns
  if ('beta' %in% colnames(left_ss) && 'se' %in% colnames(left_ss)) {
    left_ss = left_ss %>%
      dplyr::select(variant_id,
                    left_beta = beta,
                    left_se = se,
                    left_eaf = eaf)
  } else {
    left_ss = left_ss %>%
      dplyr::select(variant_id,
                    left_pval = pval,
                    left_eaf = eaf)
  }
  
  if ('beta' %in% colnames(right_ss) && 'se' %in% colnames(right_ss)) {
    right_ss = right_ss %>%
      dplyr::select(variant_id,
                    right_beta = beta,
                    right_se = se,
                    right_eaf = eaf)
  } else {
    right_ss = right_ss %>%
      dplyr::select(variant_id,
                    right_pval = pval,
                    right_eaf = eaf)
  }
  
  # Merge left & right sumstats to ensure variants are in identical order
  merged_ss = left_ss %>% dplyr::inner_join(right_ss, by='variant_id')
  
  # EAF to MAF function
  eaf_to_maf = function(eaf) {
    min(eaf, 1-eaf)
  }
  
  # Make coloc dataset (left)
  # If beta/se columns are available, use them
  left_data = list(
    N = left_n,
    MAF = sapply(merged_ss$left_eaf, eaf_to_maf),
    type = left_type
  )
  if ('left_beta' %in% colnames(merged_ss) && 'left_se' %in% colnames(merged_ss)) {
    left_data$beta = merged_ss$left_beta
    left_data$varbeta = merged_ss$left_se^2
  } else {
    left_data$pvalues = merged_ss$left_pval
  }
  if (left_type == 'cc') {
    left_data$s = left_prop
  }
  
  # Make coloc dataset (right)
  right_data = list(
    N=right_n,
    MAF=sapply(merged_ss$right_eaf, eaf_to_maf),
    type=right_type
  )
  if ('right_beta' %in% colnames(merged_ss) && 'right_se' %in% colnames(merged_ss)) {
    right_data$beta = merged_ss$right_beta
    right_data$varbeta = merged_ss$right_se^2
  } else {
    right_data$pvalues = merged_ss$right_pval
  }
  if (right_type == 'cc') {
    right_data$s = right_prop
  }
  
  # Run coloc
  coloc_res = coloc::coloc.abf(left_data,
                               right_data,
                               p1 = 1e-04, p2 = 1e-04, p12 = 1e-05)
  
  # Output coloc result
  coloc_summ = data.frame(t(coloc_res$summary))
  readr::write_csv(coloc_summ, outfile, col_names=T)
  
  # Output full results
  # coloc_full = data.frame(coloc_res$results)
  # out_f = paste0(outpref, '.full.tsv.gz')
  # write_tsv(coloc_full, out_f, col_names=T)
}


main()
