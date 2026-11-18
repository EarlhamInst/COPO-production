'''Unit tests for reading Webin-CLI's validation reports.

Webin-CLI writes '<manifest>.report' next to the manifest for manifest errors,
and '<outputDir>/<context>/<name>/validate/*.report' for file validation errors.
get_webin_cli_report collects the ERROR and WARNING lines from all of them, so
users see why ENA rejected a submission without Java stack traces.

To execute these tests, run: $ pytest test/unit/ena_utils/test_webin_cli_report.py
'''

from common.ena_utils.ena_helper import get_webin_cli_report


def test_errors_and_warnings_are_collected_from_every_report(tmp_path):
    (tmp_path / 'manifest.txt.report').write_text('ERROR: Unknown field: AGP\n')
    validate_dir = tmp_path / 'annotation' / 'genes_annotation1' / 'validate'
    validate_dir.mkdir(parents=True)
    (validate_dir / 'genes.gff3.gz.report').write_text(
        'INFO: Validating genes.gff3.gz\n'
        'WARNING: Missing locus_tag on gene1\n'
        '\tat uk.ac.ebi.ena.webin.cli.WebinCli.main(WebinCli.java:1)\n'
        'ERROR: Invalid phase on CDS\n'
    )

    report = get_webin_cli_report(tmp_path)

    assert report.split('<br>') == [
        # Sorted by path, so the 'annotation/...' report comes before 'manifest.txt.report'
        'WARNING: Missing locus_tag on gene1',
        'ERROR: Invalid phase on CDS',
        'ERROR: Unknown field: AGP',
    ]


def test_no_reports_gives_an_empty_string(tmp_path):
    assert get_webin_cli_report(tmp_path) == ''


def test_reports_without_errors_or_warnings_give_an_empty_string(tmp_path):
    (tmp_path / 'manifest.txt.report').write_text('INFO: The submission has been validated successfully.\n')

    assert get_webin_cli_report(tmp_path) == ''
