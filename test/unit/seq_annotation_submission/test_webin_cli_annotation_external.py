'''External tests: GFF3 annotations submissions to ENA

- PUBLIC_GCA:      a public assembly owned by the Webin account (should work)
- OTHER_OWNER_GCA: a public assembly owned by a different Webin account

Leave a GCA blank to skip its test. Set EXPECT_ACCEPTED to what you expect ENA
to do. When a test fails, the message includes Webin-CLI's output and report,
which says why ENA rejected (or unexpectedly accepted) the annotation.

By default the tests only validate (-validate), which never creates anything
at ENA. Set ALSO_SUBMIT = True to submit as well; this is only allowed when
ENA_SERVICE points at ENA's dev server, where Webin-CLI runs with -test and
ENA discards test submissions after 24 hours.

Requirements: java, tools/reposit/ena_cli/webin-cli.jar and the WEBIN_USER
and WEBIN_USER_PASSWORD environment variables (or their *_FILE variants).

These are opt-in. A plain `pytest` skips them; run them with:
    $ pytest test/unit/seq_annotation_submission/test_webin_cli_annotation_external.py -m external -s
'''

import gzip
import os
import shutil
import uuid
from pathlib import Path
from unittest import mock

import pytest
from django.test import override_settings

from common.ena_utils import ena_helper
from common.lookup.lookup import ENA_CLI
from src.apps.copo_seq_annotation_submission.utils import EnaAnnotation

# ── Fill these in ───────────────────────────────────────────────────────

# A study (PRJEB...) owned by the Webin account. ENA requires the annotation's
# study to belong to the submitter.
STUDY_ACCESSION = 'PRJEB101635' # Study for Golden Jackal genome annotation

# Each case: the GCA accession (with version, e.g. 'GCA_977927465.1'), and
# optionally a path to a GFF3 file (.gff3 or .gff3.gz) whose sequence names
# match that assembly. Paths are relative to the repo root, with or without a
# leading '/' (e.g. 'test/testfiles/x.gff3.gz'), unless the file exists at the
# exact absolute path given. Without one, a small built-in GFF3 is used; its
# sequence name 'chr1' won't match most assemblies, so ENA may reject it for
# that reason alone.
PUBLIC_GCA = 'GCA_977927465.2'
PUBLIC_GCA_GFF3 = 'test/testfiles/test_annotation_mcanaur2_v2.gff3.gz'

OTHER_OWNER_GCA = 'GCA_907163345.1'
OTHER_OWNER_GCA_GFF3 = 'test/testfiles/test_annotation_hrh.gff3.gz'

# What you expect ENA to do for each case. Change these once you know ENA's rules.
EXPECT_ACCEPTED = {
    'public': True,
    'private': True,
    'other_owner': True,
}

# Also run -submit after a successful -validate (ENA dev server only)
ALSO_SUBMIT = False

# NB: ENA data files are found here: https://www.ebi.ac.uk/ena/portal/api/search?result=analysis&query=analysis_type="SEQUENCE_ANNOTATION"&fields=analysis_accession,scientific_name,submitted_format,submitted_ftp&format=tsv

# ────────────────────────────────────────────────────────────────────────

DEFAULT_GFF3 = (
    b'##gff-version 3\n'
    b'##sequence-region chr1 1 15000\n'
    b'chr1\t.\tgene\t100\t900\t.\t+\t.\tID=gene1;locus_tag=COPOTEST_000001\n'
)

CASES = [
    pytest.param('public', PUBLIC_GCA, PUBLIC_GCA_GFF3, id='public_gca_owned_by_us'),
    pytest.param('other_owner', OTHER_OWNER_GCA, OTHER_OWNER_GCA_GFF3, id='public_gca_owned_by_someone_else'),
]

pytestmark = pytest.mark.external


@pytest.fixture(autouse=True)
def only_when_asked_for(request):
    # pytest.ini's testpaths makes a bare `pytest` collect this file, so skip
    # unless the run explicitly selected external tests
    markexpr = request.config.option.markexpr or ''
    if 'external' not in markexpr or 'not external' in markexpr:
        pytest.skip('External test: run with -m external')
    if not shutil.which('java'):
        pytest.skip('java is not installed')
    if not Path(ENA_CLI).is_file():
        pytest.skip(f'Webin-CLI jar not found at {ENA_CLI}')
    if not (ena_helper.user_token and ena_helper.pass_word):
        pytest.skip('WEBIN_USER and WEBIN_USER_PASSWORD are not set')
    if not STUDY_ACCESSION:
        pytest.skip('Set STUDY_ACCESSION at the top of this file')
    if ALSO_SUBMIT and 'dev' not in ena_helper.ena_service:
        pytest.fail('ALSO_SUBMIT would create a real submission: ENA_SERVICE is not the dev server')


REPO_ROOT = Path(__file__).resolve().parents[3]


def resolve_path(path):
    '''Find a file given as an absolute path, or relative to the repo root (leading '/' optional).'''
    path = Path(path)
    if path.is_absolute() and path.is_file():
        return path
    return REPO_ROOT / str(path).lstrip('/')


def write_gff3(gff3_path, work_dir):
    '''Return a gzipped GFF3 file in work_dir, as COPO stores it before submission.'''
    target = work_dir / 'annotation.gff3.gz'
    if not gff3_path:
        target.write_bytes(gzip.compress(DEFAULT_GFF3))
    elif str(gff3_path).endswith('.gz'):
        shutil.copy(gff3_path, target)
    else:
        target.write_bytes(gzip.compress(Path(gff3_path).read_bytes()))
    return target


@pytest.fixture
def run_copo_submission(tmp_path):
    '''Run EnaAnnotation.submit_decoupled_annotation with real Webin-CLI.

    Returns a function taking (gca, gff3_path) and returning what happened:
    the Webin-CLI calls made, and the accession or error COPO saved.
    '''
    def run(gca, gff3_path):
        annotation_id = f'copotest{uuid.uuid4().hex[:12]}'  # ENA needs a new NAME each time
        gff3 = write_gff3(gff3_path, tmp_path)
        seq_annotation = {
            '_id': annotation_id,
            'title': 'COPO external test',
            'study': STUDY_ACCESSION,
            'primary_id': gca,
            'description': f'COPO Webin-CLI external test against {gca}',
            'files': ['file1'],
        }
        sub = {'_id': 'sub1', 'profile_id': 'profile1'}
        calls = []
        real_run_webin_cli = ena_helper.EnaSubmissionHelper.run_webin_cli

        def run_webin_cli(helper, manifest_path, context, action, output_dir=None):
            if action == 'submit' and not ALSO_SUBMIT:
                calls.append(('submit', None, 'skipped (ALSO_SUBMIT is False)'))
                return 1, 'submit skipped'
            return_code, output = real_run_webin_cli(helper, manifest_path, context, action, output_dir)
            calls.append((action, return_code, output))
            return return_code, output

        with override_settings(MEDIA_ROOT=str(tmp_path)), \
                mock.patch.object(ena_helper, 'Profile'), \
                mock.patch.object(ena_helper, 'ghlper'), \
                mock.patch.object(ena_helper.EnaSubmissionHelper, 'run_webin_cli', run_webin_cli), \
                mock.patch.object(EnaAnnotation, 'l'), \
                mock.patch.object(EnaAnnotation, 'notify_annotation_status'), \
                mock.patch.object(EnaAnnotation, 'Submission'), \
                mock.patch.object(EnaAnnotation, 'DataFile') as data_file, \
                mock.patch.object(EnaAnnotation, 'SequenceAnnotation') as seq_annotation_da:
            data_file.return_value.get_record.return_value = {'file_location': str(gff3)}
            EnaAnnotation.submit_decoupled_annotation(seq_annotation, sub)

        work_dir = tmp_path / 'ena_annotation_files' / sub['profile_id'] / annotation_id
        da = seq_annotation_da.return_value
        accession = da.add_accession.call_args.args[1] if da.add_accession.called else None
        error = da.update_seq_annotation_error.call_args.args[2] if da.update_seq_annotation_error.called else None
        return calls, accession, error, (work_dir / 'manifest.txt').read_text()

    return run


def describe(gca, calls, accession, error, manifest):
    lines = [f'GCA: {gca}', 'Manifest:', manifest]
    for action, return_code, output in calls:
        lines += [f'-{action}: return code {return_code}', output]
    lines += [f'Accession saved: {accession}', f'Error saved: {error}']
    return '\n'.join(lines)


@pytest.mark.parametrize('case, gca, gff3_path', CASES)
def test_webin_cli_on_annotation_for_gca(run_copo_submission, case, gca, gff3_path):
    if not gca:
        pytest.skip(f'Set the {case} GCA accession at the top of this file')
    if gff3_path:
        gff3_path = resolve_path(gff3_path)
        if not gff3_path.is_file():
            pytest.fail(f'GFF3 file not found: {gff3_path}')

    calls, accession, error, manifest = run_copo_submission(gca, gff3_path)
    details = describe(gca, calls, accession, error, manifest)
    print(f'\n── {case} ──\n{details}')  # visible with -s

    assert f'PRIMARY_ID\t{gca}' in manifest
    validate_return_code = calls[0][1]
    accepted = validate_return_code == 0
    if ALSO_SUBMIT and accepted:
        accepted = accession is not None and accession.startswith('ERZ')

    expected = EXPECT_ACCEPTED[case]
    assert accepted == expected, (
        f'Expected ENA to {"accept" if expected else "reject"} the annotation, '
        f'but it was {"accepted" if accepted else "rejected"}.\n{details}')
