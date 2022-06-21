version 1.0

struct Pgen {
    File pgen
    File psam
    File pvar
}

workflow vcfToPgen{
    input {
        Array[File] input_vcfs
        String output_prefix
    }

    scatter(input_vcf in input_vcfs){
        call convertVcfToPgen as convert {
            input :
                input_vcf = input_vcf,
                output_prefix = output_prefix
        }
    }

    output {
        Array[Pgen] pgens = convert.pgen
    }
}

task convertVcfToPgen {
    input {
        File input_vcf

        String output_prefix
        Int? preemptible_tries
        Int disk_size = ceil (3 * size(input_vcf, "GB")) + 20
    }

    meta {
        description: "Convert a vcf to pgen"
    }

    String name = basename(basename(input_vcf, ".gz"),".vcf")
    command <<<
        set -e

        plink2 \
          --vcf ~{input_vcf} \
          --make-pgen vzs \
          --out ~{output_prefix}.~{name}
    >>>

    runtime {
        preemptible: select_first([preemptible_tries, 5])
        memory: "3 GB"
        disks: "local-disk " + disk_size + " HDD"
        docker: "us.gcr.io/broad-dsde-methods/plink2-alpha"
    }

    output {
    Pgen pgen= {
                   "pgen" : "~{output_prefix}.~{name}.pgen",
                   "psam" : "~{output_prefix}.~{name}.psam",
                   "pvar": "~{output_prefix}.~{name}.pvar.zst"
               }
    }
}
