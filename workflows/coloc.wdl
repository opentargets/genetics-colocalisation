# Workflow to combine tasks together
workflow coloc {

  File inputManifestFile
  Array[Array[String]] inputManifest = read_tsv(inputManifestFile)

  scatter (rec in inputManifest) {
    call coloc_single {
      input:
        left_sumstats=rec[0],
        left_ld=rec[1],
        left_study_id=rec[2],
        left_type=rec[3],
        left_phenotype_id=rec[4],
        left_biofeature=rec[5],
        left_lead_chrom=rec[6],
        left_lead_pos=rec[7],
        left_lead_ref=rec[8],
        left_lead_alt=rec[9],
        right_sumstats=rec[10],
        right_ld=rec[11],
        right_study_id=rec[12],
        right_type=rec[13],
        right_phenotype_id=rec[14],
        right_biofeature=rec[15],
        right_lead_chrom=rec[16],
        right_lead_pos=rec[17],
        right_lead_ref=rec[18],
        right_lead_alt=rec[19],
        method=rec[20],
        out=rec[21],
        log=rec[22],
        tmpdir=rec[23],
        plot=rec[24]
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
  String left_sumstats
  String left_type
  String left_study_id
  String? left_phenotype_id
  String? left_biofeature
  String left_lead_chrom
  Int left_lead_pos
  String left_lead_ref
  String left_lead_alt
  String right_sumstats
  String right_type
  String right_study_id
  String? right_phenotype_id
  String? right_biofeature
  String right_lead_chrom
  Int right_lead_pos
  String right_lead_ref
  String right_lead_alt
  String r_coloc_script
  String method
  String? top_loci
  String? left_ld
  String? right_ld
  Int window_coloc
  Int? window_cond
  Float? min_maf
  String out
  String? plot
  String log
  String tmpdir

  command {
    python ${script} \
      --left_sumstat ${left_sumstats} \
      --left_type ${left_type} \
      --left_study ${left_study_id} \
      --left_phenotype ${default='None' left_phenotype_id} \
      --left_biofeature ${default='None' left_biofeature} \
      --left_chrom ${left_lead_chrom} \
      --left_pos ${left_lead_pos} \
      --left_ref ${left_lead_ref} \
      --left_alt ${left_lead_alt} \
      --right_sumstat ${right_sumstats} \
      --right_type ${right_type} \
      --right_study ${right_study_id} \
      --right_phenotype ${default='None' right_phenotype_id} \
      --right_biofeature ${default='None' right_biofeature} \
      --right_chrom ${right_lead_chrom} \
      --right_pos ${right_lead_pos} \
      --right_ref ${right_lead_ref} \
      --right_alt ${right_lead_alt} \
      --r_coloc_script ${r_coloc_script} \
      --method ${method} \
      --top_loci ${default='None' top_loci} \
      --left_ld ${default='None' left_ld} \
      --right_ld ${default='None' right_ld} \
      --window_coloc ${window_coloc} \
      --window_cond ${default='None' window_cond} \
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
