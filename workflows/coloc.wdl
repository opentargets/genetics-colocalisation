# Workflow to combine tasks together
workflow coloc {

  File inputManifestFile
  Array[Array[String]] inputManifest = read_tsv(inputManifestFile)

  scatter (rec in inputManifest) {
    call coloc_single {
      input:
        sumstat_left
        study_left
        cell_left
        group_left
        trait_left
        chrom_left
        pos_left
        ref_left
        alt_left
        ld_left
        sumstat_right
        study_right
        cell_right
        group_right
        trait_right
        chrom_right
        pos_right
        ref_right
        alt_right
        method
        top_loci
        ld_right
        out
        plot
        log
        tmpdir
    }
  }
}


0 sumstat_left
1 study_left
2 cell_left
3 group_left
4 trait_left
5 chrom_left
6 pos_left
7 ref_left
8 alt_left
9 ld_left
10 sumstat_right
11 study_right
12 cell_right
13 group_right
14 trait_right
15 chrom_right
16 pos_right
17 ref_right
18 alt_right
19 ld_right
20 method
21 out
22 plot
23 log
24 tmpdir

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
