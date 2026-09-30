"""Choosing the ENA run.xml filetype for a read file.

Deliberately free of imports with side effects (no Django settings, no env
lookups) so it can be unit tested on its own: importing ena_helper reads
WEBIN_USER at module level and raises IndexError where that is unset.
"""

import os

# FILE/@filetype values accepted by ENA's run XML (SRA.run.xsd). The manifest's
# "File Type" column is an enum constrained to a subset of these, so a declared
# value only has to be recognised, never translated.
ENA_RUN_FILETYPES = (
    "fastq", "bam", "cram", "sff", "srf", "sra", "fasta", "tab",
    "OxfordNanopore_native", "PacBio_HDF5", "CompleteGenomics_native",
    "Helicos_native", "Illumina_native", "SOLiD_native", "454_native",
)
_ENA_RUN_FILETYPES_BY_LOWER = {value.lower(): value for value in ENA_RUN_FILETYPES}


def ena_run_filetype(file_name, declared_type=""):
    """The ENA run.xml filetype to declare for a read file.

    Prefer what the submitter chose in the manifest ("File Type"), because the
    extension cannot express it: an Oxford Nanopore submission is a tar.gz of
    fast5 files, and ENA rejects that unless it is declared
    OxfordNanopore_native. COPO used to send "fastq" for everything that was
    not .bam/.cram, which cost a 616GB upload its registration:

        Invalid file suffix for file "fast5.tar.gz".
        File archival is not allowed for file type "fastq".

    Falls back to the old extension check when nothing usable is declared —
    rows reach here from a dataframe, so declared_type may be NaN or absent.
    """
    if isinstance(declared_type, str):
        canonical = _ENA_RUN_FILETYPES_BY_LOWER.get(declared_type.strip().lower())
        if canonical:
            return canonical

    _, file_extension = os.path.splitext(file_name or "")
    if file_extension in (".cram", ".bam"):
        return file_extension[1:]
    return "fastq"
