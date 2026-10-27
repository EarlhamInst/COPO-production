import re
import time

import pytest
from playwright.sync_api import Page

from test.playwright.e2e.utils.edp_helpers import (
    EDP_PROFILE_FIELDS,
    create_edp_profile,
    edp_jira_ticket_number,
    read_sapio_project,
)
from test.playwright.e2e.utils.helpers import (
    create_biodata_profile,
    delete_profile,
    delete_profile_by_title,
)

# Both profile titles and descriptions must be at least 20 characters long
TITLE = 'Playwright {profile_type} Profile'
DESCRIPTION = 'A description that is at least twenty characters long.'


def test_create_and_delete_biodata_profile(page: Page):
    create_biodata_profile(page, TITLE.format(profile_type='Biodata'), DESCRIPTION)
    delete_profile(page, TITLE.format(profile_type='Biodata'))


@pytest.mark.external
def test_create_and_delete_edp_profile(page: Page, sapio_test_instance):
    # An EDP profile is mirrored in Sapio: saving it creates a Sapio project
    # with one sample per 'Number of samples', and deleting it deletes that
    # project. Check both sides in Sapio itself, not just the COPO card.
    #
    # A leftover from an interrupted run would otherwise satisfy the card
    # check below without a new profile being created; deleting it this way
    # also removes its Sapio project.
    edp_profile_title = TITLE.format(profile_type='EDP')
    delete_profile_by_title(edp_profile_title)

    jira_ticket_number = edp_jira_ticket_number()
    _, _, project_id = create_edp_profile(
        page, edp_profile_title, DESCRIPTION, jira_ticket_number=jira_ticket_number, **EDP_PROFILE_FIELDS
    )
    assert re.fullmatch(r'PR\d+', project_id), f'Unexpected Sapio project id {project_id!r}'

    sapio = read_sapio_project(
        project_id,
        project_fields=['ProjectName', 'C_BudgetHolder', 'C_SampleCount'],
        sample_fields=['SampleId'],
    )
    assert sapio['exists'], f'Sapio project {project_id} was not created'
    assert sapio['project']['ProjectName'] == jira_ticket_number
    assert sapio['project']['C_BudgetHolder'] == EDP_PROFILE_FIELDS['budget_user']
    assert int(sapio['project']['C_SampleCount']) == EDP_PROFILE_FIELDS['no_of_samples']
    assert len(sapio['samples']) == EDP_PROFILE_FIELDS['no_of_samples']

    delete_profile(page, edp_profile_title)

    # delete_project() only logs a warning if Sapio refuses the delete, so
    # the profile disappearing from COPO doesn't prove the project went too.
    deadline = time.time() + 60
    while read_sapio_project(project_id)['exists']:
        assert time.time() < deadline, f'Sapio project {project_id} still exists after deleting the profile'
        time.sleep(5)
