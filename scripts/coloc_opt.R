#!/usr/bin/env Rscript
#
#library("dplyr", include.only = 'select')
#library("dplyr", include.only = 'bind_cols')
#library("dplyr", include.only = 'read_tsv')
#library("dplyr", include.only = 'write_tsv')
#library("data.table", include.only = 'fread')
library("magrittr", include.only = '%>%')

main = function() {
  args = commandArgs(trailingOnly=TRUE)
  manifest_file = args[1]
  output_file = args[2]

  start_time = Sys.time()
  
  print(sprintf("Start time: %s", start_time))
  print(sprintf("Processing manifest file: %s", manifest_file))

  # Make directory if it doesn't exist
  if (!dir.exists(dirname(output_file))) {
    dir.create(dirname(output_file), recursive = TRUE, showWarnings = FALSE)
  }

  manifest = readr::read_tsv(manifest_file)
  print(sprintf("Running %d coloc tests from manifest", nrow(manifest)))
  
  # Make a dataframe of data that we're going to join to each coloc result
  manifest_join = manifest %>%
    dplyr::select(left_type,
           left_study,
           left_bio_feature,
           left_phenotype,
           left_chrom,
           left_pos,
           left_ref,
           left_alt,
           right_type,
           right_study,
           right_bio_feature,
           right_phenotype,
           right_chrom,
           right_pos,
           right_ref,
           right_alt)
  
  if (file.exists(output_file)) {
    print(sprintf("Coloc output file %s already exists. Overwriting.", output_file))
    file.remove(output_file)
  }
  
  error_count = 0
  # Rprof()
  
  # Run all coloc tests in the manifest, and write to the output file
  for (i in 1:nrow(manifest)) {
    # Run coloc, and catch any errors and continue
    tryCatch(
      {
        print(sprintf("Coloc left sumstat,right sumstat,outfile\n%s,%s,%s",
                      manifest$left_reduced_sumstats[i], manifest$right_reduced_sumstats[i], output_file))
        
        coloc_summary = do_coloc(manifest$left_reduced_sumstats[i],
                                 manifest$right_reduced_sumstats[i])
        
        # Add columns with info on the coloc test that was done
        coloc_summary = dplyr::bind_cols(coloc_summary, manifest_join[i,])

        # Write coloc output; only write colnames for the first one
        write_colnames = (i == 1)
        readr::write_csv(coloc_summary, output_file, col_names=write_colnames, append=T)
      },
      error = function(e) {
        error_count <<- error_count + 1
        print(e)
        print(e$message)
        print(e$trace)
      }
    )
    
    # Output any warnings from this coloc test
    warnings()
  }
  
  print(sprintf("Completed processing manifest file %s. Num errors caught: %d", manifest_file, error_count))
  
  end_time <- Sys.time()
  print(sprintf("End time: %s", start_time))
  print(sprintf("Time taken: %s", format(end_time - start_time)))

  # Rprof(NULL)
  # print(summaryRprof())
}


do_coloc = function(in_left_ss, in_right_ss) {
  # Load data
  left_ss = data.table::fread(in_left_ss) %>%
    dplyr::filter(!is.na(beta))
  
  right_ss = data.table::fread(in_right_ss) %>%
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
  
  # Return coloc result summary
  coloc_summary = data.frame(t(coloc_res$summary))
  return(coloc_summary)
  
  # Output full results
  # coloc_full = data.frame(coloc_res$results)
  # out_f = paste0(outpref, '.full.tsv.gz')
  # write_tsv(coloc_full, out_f, col_names=T)
}


main()
