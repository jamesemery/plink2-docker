version 1.0

workflow vcfToPgen{
    input {
    File input_vcf
    File? input_vcf_index
    String output_prefix
    }

    call convertVcfToPgen as convert {
        input :
            input_vcf = input_vcf,
            input_vcf_index = input_vcf_index,
            output_prefix = output_prefix
    }
}

task convertVcfToPgen {
    input {
        File input_vcf
        File? input_vcf_index

        String output_prefix
        Int? preemptible_tries
        Int disk_size = ceil (3 * size(input_vcf, "GB")) + 20
    }

    meta {
        description: "Convert a vcf to pgen"
    }
    command <<<
        set -e

        plink2 \
          --vcf ~{input_vcf} \
          --make-pgen vzs \
          --out ~{output_prefix}
    >>>

    runtime {
        preemptible: select_first([preemptible_tries, 5])
        memory: "3 GB"
        disks: "local-disk " + disk_size + " HDD"
        docker: "us.gcr.io/broad-dsde-methods/plink2-alpha"
    }

    output {
        File pgen = "${output_prefix}.pgen"
        File psam = "${output_prefix}.psam"
        File pvar = "${output_prefix}.pvar.zst"
    }
}
