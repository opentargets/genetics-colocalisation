# Workflow to combine tasks together
workflow coloc {

  File inputManifestFile
  Array[Array[String]] inputManifest = read_tsv(inputManifestFile)

  scatter (rec in inputManifest) {
    call coloc_single {
      input:
        pq=study[0],
        ld=study[1],
        study_id=study[2],
        cell_id=study[3],
        group_id=study[4],
        trait_id=study[5],
        chrom=study[6],
        method=study[7],
        toploci=study[8],
        credset=study[9],
        log=study[10],
        tmpdir=study[11]
    }
  }
}



# Task to run the coloc script
task coloc_single {
  String script
  String sumstat_left
  String study_left
  String? cell_left
  String? group_left
  String trait_left
  String chrom_left
  Int pos_left
  String ref_left
  String alt_left
  String sumstat_right
  String study_right
  String cell_right
  String group_right
  String trait_right
  String chrom_right
  Int pos_right
  String ref_right
  String alt_right
  String method
  String? top_loci
  String? ld_left
  String? ld_right
  Int window_coloc
  Int window_cond
  Float? min_maf
  String out
  String? plot
  String log
  String tmpdir

  command {
    python ${script} \
      --sumstat_left ${sumstat_left} \
      --study_left ${study_left} \
      --cell_left ${default='None' cell_left} \
      --group_left ${default='None' group_left} \
      --trait_left ${trait_left} \
      --chrom_left ${chrom_left} \
      --pos_left ${pos_left} \
      --ref_left ${ref_left} \
      --alt_left ${alt_left} \
      --sumstat_right ${sumstat_right} \
      --study_right ${study_right} \
      --cell_right ${cell_right} \
      --group_right ${group_right} \
      --trait_right ${trait_right} \
      --chrom_right ${chrom_right} \
      --pos_right ${pos_right} \
      --ref_right ${ref_right} \
      --alt_right ${alt_right} \
      --method ${method} \
      --top_loci ${default='None' top_loci} \
      --ld_left ${default='None' ld_left} \
      --ld_right ${default='None' ld_right} \
      --window_coloc ${window_coloc} \
      --window_cond ${window_cond} \
      --min_maf ${default='None' min_maf} \
      --out ${out} \
      --plot ${default='None' plot} \
      --log ${log} \
      --tmpdir ${tmpdir}
  }
  output {
    File coloc_res = "${out}"
  }
}



/* # Workflow combining tasks together
workflow finemapping {
  File inputManifestFile
  Array[Array[String]] inputManifest = read_tsv(inputManifestFile)
  scatter (study in inputManifest) {
    call finemap_single_study {
      input:
        pq=study[0],
        ld=study[1],
        study_id=study[2],
        cell_id=study[3],
        group_id=study[4],
        trait_id=study[5],
        chrom=study[6],
        method=study[7],
        toploci=study[8],
        credset=study[9],
        log=study[10],
        tmpdir=study[11]
    }
  }
  call concat_parquets as concat_toploci {
    input:
      in=finemap_single_study.toploci_res
  }
  call concat_parquets as concat_credset {
    input:
      in=finemap_single_study.credset_res
  }
}

# Task to run the finemapping script
task finemap_single_study {
  String script
  String pq
  String ld
  File config_file
  String study_id
  String? cell_id
  String? group_id
  String trait_id
  String chrom
  String method
  String toploci
  String credset
  String log
  String tmpdir
  command {
    python ${script} \
      --pq ${pq} \
      --ld ${ld} \
      --config_file ${config_file} \
      --study_id ${study_id} \
      --trait_id ${trait_id} \
      --chrom ${chrom} \
      --cell_id ${default='""' cell_id} \
      --group_id ${default='""' group_id} \
      --method ${method} \
      --toploci ${toploci} \
      --credset ${credset} \
      --log ${log} \
      --tmpdir ${tmpdir}
  }
  output {
    File toploci_res = "${toploci}"
    File credset_res = "${credset}"
  }
}

# Task to load parquets into memory and write to a single file
task concat_parquets {
  String script
  Array[File] in
  String out
  command {
    python ${script} \
      --in_parquets ${sep=" " in} \
      --out ${out} \
  }
  output {
    File result = "${out}"
  }
} */
