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
        Float scatter_div_first = 2.0
        Float scatter_div_second = 2.0
    }

    scatter(input_vcf in input_vcfs){
        call convertVcfToPgen as convert {
            input :
                input_vcf = input_vcf,
                output_prefix = output_prefix,
                compress = false
        }
    }

    scatter(i in range(ceil(length(convert.pgen) / scatter_div_first))) {
       scatter(j in range(round(if scatter_div_first > length(convert.pgen) - (i * scatter_div_first) then length(convert.pgen) - (i * scatter_div_first) else scatter_div_first ))) {
         Pgen sub_arr_layer1 = convert.pgen[(round(i * scatter_div_first + j))]
       }

#       scatter(k in range(ceil(length(sub_arr_layer1) / scatter_div_second))) {
#            scatter(j in range(round(if scatter_div_second > length(sub_arr_layer1) - (k * scatter_div_second) then length(sub_arr_layer1) - (k * scatter_div_second) else scatter_div_second ))) {
#              Pgen sub_arr_layer2 = sub_arr_layer1[(round(k * scatter_div_second + j))]
#            }
#            String uniquifiedName = "merged_"+i+"_"+k
#            call mergePgens as merge_layer_2 {
#                input :
#                    pgens = sub_arr_layer2,
#                    output_prefix = uniquifiedName,
#                    disk_size = 60
#               }
#       }

        String uniquifiedName = "merged_"+i
        call mergePgens as merge_layer_1 {
            input :
                pgens = sub_arr_layer1,
                output_prefix = uniquifiedName,
                disk_size = 300
           }
    }

    call mergePgens as mergeFinal {
        input :
            pgens = merge_layer_1.pgen,
            output_prefix = "alldone",
            disk_size = 300,
            compress = true
       }

    call mergePgens as mergeEverythinTest {
        input :
            pgens = convert.pgen,
            output_prefix = "alldone",
            disk_size = 300,
            compress = true
       }


    output {
        Array[Pgen] pgens = convert.pgen
        Pgen merged = mergeFinal.pgen
    }
}

task mergePgens {
    input {
        Array[Pgen] pgens
        Int disk_size = ceil (3 * 100) + 20
        String output_prefix
        Boolean compress = false
    }


    command <<<
        set -x

        JSON_FILE=~{write_json(pgens)}

        apt-get update
        apt-get install jq -y

        cat $JSON_FILE | jq -r '[.[] | {pgen, pvar, psam} | join(" ")] | join("\n")' > allfiles.txt

        cat allfiles.txt

        plink2  \
         --pmerge-list allfiles.txt pfile-vzs \
         ~{if compress then "--pmerge-output-vzs" else " " } \
         --out ~{output_prefix}

        ls .
    >>>

    runtime {
        memory: "3 GB"
        disks: "local-disk " + disk_size + " HDD"
        docker: "us.gcr.io/broad-dsde-methods/plink2-alpha"
    }

    output {
    Pgen pgen= {
                   "pgen" : "~{output_prefix}.pgen",
                   "psam" : "~{output_prefix}.psam",
                   "pvar": "~{output_prefix}.pvar~{if compress then '.zst' else '' }"
               }
    }

}

task convertVcfToPgen {
    input {
        File input_vcf

        String output_prefix
        Int? preemptible_tries
        Int disk_size = ceil (3 * size(input_vcf, "GB")) + 20
        Boolean compress = true
    }

    meta {
        description: "Convert a vcf to pgen"
    }

    String name = basename(basename(input_vcf, ".gz"),".vcf")
    command <<<
        set -e

        plink2 \
          --vcf ~{input_vcf} \
          --make-pgen ~{if compress then "vzs" else "" } \
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
                   "pvar": "~{output_prefix}.~{name}.pvar~{if compress then '.zst' else '' }" #"~{output_prefix}.~{name}.pvar.zst"
               }
    }
}
