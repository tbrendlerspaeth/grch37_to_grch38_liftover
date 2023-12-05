// grch37_to_38_liftover


nextflow.enable.dsl = 2

// channel of *.vcf.gz files.
zipped_ch = Channel.fromPath('/path/to/input/*.vcf.gz')

// paths to liftover reference genome, dict, and chain file.
params.reference_genome = "/path/to/Homo_sapiens.GRCh38.dna.primary_assembly.fa"
params.chain_file = "/path/to/GRCh37_to_GRCh38.chain"
params.dict_file = "/path/to/Homo_sapiens.GRCh38.dna.primary_assembly.dict"

workflow {
    // Obtain files from a folder location as a channel
    // Unzip the files and pass them to second process
    // With second process, replace the white spaces in the file and pass to another channel
    // Then, using GATK, liftover the files and publish outputs to different directories
    zipped_ch | UNZIP_FILE
    UNZIP_FILE.out | RM_WS 
    input_vcf = RM_WS.out
    LIFTOVER(params.reference_genome, params.chain_file, input_vcf, params.dict_file)
}

process UNZIP_FILE { 
    // Unzip the .gz VCF file.
    cpus 1
    memory '1 GB'
    time '3m'
    executor 'slurm'

    input:
    file(my_file)

    output:
    file(my_file.baseName)

    shell:
    """
    gzip -cd "${my_file}" > "${my_file.baseName}" 
    """
}

process RM_WS {
    // Replace all whitespaces in the VCF with underscores.
    cpus 1
    memory '1 GB'
    time '5m'
    executor 'slurm'

    input:
    file(unzipped_file)

    output:
    file("${unzipped_file.getBaseName()}.no_ws.vcf")

    shell:
    """
    sed 's/ /_/g'  "${unzipped_file}" >  "${unzipped_file.getBaseName()}.no_ws.vcf"
    """
}


process LIFTOVER {
    // Liftover the variants in the VCF from GRCh37 to GRCh38.
    // Save lifted variant VCFs, rejected variant VCFs, and 
    // command files to separate directories for later analysis.
    cpus 1
    memory '100 GB'
    time '10m'
    executor 'slurm'

    publishDir "./Output/lifted", pattern: "*.lifted.vcf", mode: 'move'
    publishDir "./Output/rejected", pattern: "*.reject_vars.vcf", mode: 'move'
    publishDir "./Output/logs/${input_vcf.getBaseName()}", pattern: "*.command.*", mode: 'move'

    input:
    path(reference_genome)
    path(chain_file) 
    path(input_vcf)
    path(dict_file)

    output:
    file("${input_vcf.getBaseName()}.lifted.vcf")
    file("${input_vcf.getBaseName()}.reject_vars.vcf")
    file(".command.*")

    shell:
    """
    module load GATK/4.9.1
    gatk --java-options "-Xmx100G" LiftoverVcf \
	I="${input_vcf}" \
	O="${input_vcf.getBaseName()}.lifted.vcf" \
	CHAIN="${chain_file}" \
	REJECT="${input_vcf.getBaseName()}.reject_vars.vcf" \
	R="${reference_genome}"
    """
}