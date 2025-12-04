MESSAGES = {
    'validation_msg_file_already_in_queue': (
        'The files <strong>{file_names}</strong> are already in the submission queue '
        'for sample <strong>{sample_name}</strong>.'
    ),
    'validation_msg_file_must_be_single': (
        'Expected a single file for BAM or CRAM; <strong>{file_name}</strong> appears to be paired.'
    ),
    'validation_msg_file_invalid_fastq': (
        'Expected a FASTQ file for <strong>{file_name}</strong> because '
        'it uses the strong>.gz</strong> or <strong>.bz2</strong> extension.'
    ),
    'validation_msg_file_invalid_name': (
        f'''
            Invalid file name <strong>{{file_name}}</strong>.<br>
            Expected one of 
            <span class="valid-enum-trigger" 
                data-content="<ul>
                        <li><strong>.gz</strong> for FASTQ files</li>
                        <li><strong>.bam</strong> or <strong>.bz2</strong> for BAM files</li>
                        <li><strong>.cram</strong> or <strong>.bz2</strong> for CRAM files</li>
                    </ul>">
                3 valid file formats (click to view)
            </span>.
        '''
    ),
    'validation_msg_sample_duplication_error': (
        'The file, <strong>{filename}</strong>, '
        'is already linked to sample <strong>{existing_sample_name}</strong> '
        'in profile <strong class="title-ellipsis">{existing_sample_profile_title}</strong>'
    ),
    'validation_msg_single_file_error': (
        'The <strong>Library layout</strong> column indicates that only a <strong>single</strong> '
        'file and checksum are allowed but, multiple values were found in either the '
        '<strong>File name</strong> or <strong>File checksum</strong> column at row <strong>%s</strong>'
    ),
    'validation_msg_paired_file_error': (
        'Expected two filenames (in the <strong>File name</strong> column) and '
        'two checksums (in the <strong>File checksum</strong> column) because '
        '<strong>PAIRED</strong> was selected in the <strong>Library layout</strong> column '
        'at row <strong>%s</strong>.'
    ),
}
