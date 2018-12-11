# Workflow to combine tasks together
workflow coloc {

  File inputManifestFile
  Array[Array[String]] inputManifest = read_tsv(inputManifestFile)

  scatter (rec in inputManifest) {
    call coloc_single {
      input:
        sumstat_left=rec[0],
        study_left=rec[1],
        cell_left=rec[2],
        group_left=rec[3],
        trait_left=rec[4],
        chrom_left=rec[5],
        pos_left=rec[6],
        ref_left=rec[7],
        alt_left=rec[8],
        ld_left=rec[9],
        sumstat_right=rec[10],
        study_right=rec[11],
        cell_right=rec[12],
        group_right=rec[13],
        trait_right=rec[14],
        chrom_right=rec[15],
        pos_right=rec[16],
        ref_right=rec[17],
        alt_right=rec[18],
        ld_right=rec[19],
        method=rec[20],
        out=rec[21],
        plot=rec[22],
        log=rec[23],
        tmpdir=rec[24]
    }
  }

  call concat {
  input:
    in=coloc_single.coloc_res
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
  String r_coloc_script
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
      --r_coloc_script ${r_coloc_script} \
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

# Task concat the json outputs together
task concat {
  String script
  Array[File] in
  String out
  command {
    python ${script} --in_json ${sep=" " in} --out ${out}
  }
  output {
    File result = "${out}"
  }
}
