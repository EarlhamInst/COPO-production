import subprocess
import json
import datetime
from common.utils.logger import Logger
import requests
import unicodedata
import common.schemas.utils.data_utils as d_utils


l = Logger()

# HTML error message templates for taxonomy validation failures.
# Used by check_taxon_ena_submittable() to produce user-facing error strings.
MESSAGE = {
    # Shown when a taxon has a scientific name that is not a valid binomial (genus + species).
    'validation_msg_invalid_binomial_name': "For the TAXON_ID,  <strong>%s</strong>, the scientific name, <strong>%s</strong>, is not a valid binomial name. "
                                            "Please contact <a href='mailto:ena-asg@ebi.ac.uk'>ena-asg@ebi.ac.uk</a> or "
                                            "<a href='mailto:ena-dtol@ebi.ac.uk'>ena-dtol@ebi.ac.uk</a> or "
                                            "<a href='mailto:ena-bge@ebi.ac.uk'>ena-bge@ebi.ac.uk</a> to request assistance for this taxonomy.",
    # Shown when ENA marks the taxon as not submittable (e.g. environmental samples, metagenomes).
    'validation_msg_not_submittable_taxon': "TAXON_ID <strong>%s</strong> is not 'submittable' to ENA. Please see "
                                            "<a href='https://ena-docs.readthedocs.io/en/latest/faq/taxonomy_requests.html#creating-taxon-requests'>here</a> "
                                            "and contact <a href='mailto:ena-asg@ebi.ac.uk'>ena-asg@ebi.ac.uk</a> or "
                                            "<a href='mailto:ena-dtol@ebi.ac.uk'>ena-dtol@ebi.ac.uk</a> or "
                                            "<a href='mailto:ena-bge@ebi.ac.uk'>ena-bge@ebi.ac.uk</a> to request an "
                                            "informal placeholder species name. Please also refer to the ASG/DTOL/ERGA SOP.",
}

def validate_date(date_text):
    """
    Validate that date_text is a well-formed YYYY-MM-DD date in the past.

    Raises ValueError if the format is wrong, AssertionError if the date is
    today or in the future.
    """
    try:
        datetime.datetime.strptime(date_text, '%Y-%m-%d')
    except ValueError:
        raise ValueError("Incorrect data format, should be YYYY-MM-DD")
    todayis = datetime.date.today()
    enteredtime = datetime.datetime.strptime(date_text, '%Y-%m-%d').date()
    try:
        assert todayis > enteredtime
    except AssertionError:
        raise AssertionError("Incorrect date entered: date is in the future")

def check_biocollection(voucher_id, qualifier_type):
    """
    Validate a specimen voucher against the EBI Sample Accessioning Hub (SAH) API.

    Returns True if the voucher_id/qualifier_type combination is registered,
    False otherwise (including on network errors).
    """
    url = f"https://www.ebi.ac.uk/ena/sah/api/validate"

    try:
        response = requests.get(url,params={"value":voucher_id, "qualifier_type":qualifier_type})
        if response.status_code == requests.codes.ok:
            is_success = response.json().get('success')
            if is_success:
                return True
            else:
                l.debug( f"{voucher_id} for {qualifier_type} not registered" )
        else:
            l.error(str(response.status_code) + ":" + response.text)
    except Exception as e:
        l.exception(e)
    return False


def check_taxon_ena_submittable(taxon, is_binomial_required=True, by="id"):
    """
    Query the ENA taxonomy REST API to verify a taxon is valid and submittable.

    Args:
        taxon: Taxon ID (string) or scientific name depending on `by`.
        is_binomial_required: If True, also check that the name is a binomial.
        by: "id" to look up by numeric taxon ID, "binomial" by scientific name.

    Returns:
        (errors, taxinfo) — errors is a list of HTML error strings (empty = valid),
        taxinfo is the raw dict returned by ENA (or None on failure).

    Checks performed:
        - ENA returns a result (taxon exists)
        - taxinfo["submittable"] == "true"
        - taxinfo["rank"] is "species" or "subspecies"
        - If is_binomial_required: taxinfo["binomial"] is True
    """
    errors = []
    body = ""
    taxinfo = None

    # Use `requests` (not subprocess+curl) so the call is non-blocking under
    # gevent — a synchronous curl blocks the whole event loop, stalling every
    # other concurrent celery task until the request returns.
    if by == "id":
        url = "https://www.ebi.ac.uk/ena/taxonomy/rest/tax-id/" + taxon
    elif by == "binomial":
        url = "https://www.ebi.ac.uk/ena/taxonomy/rest/scientific-name/" + taxon.replace(" ", "%20")
    else:
        errors.append(MESSAGE['validation_msg_not_submittable_taxon'] % (taxon))
        return errors, taxinfo

    try:
        resp = requests.get(url, timeout=10)
        body = resp.text or ""

        if body.strip() == "" or body.strip() == "No results.":
            errors.append("ENA returned no results for " + taxon)
            return errors, taxinfo

        taxinfo = json.loads(body)

        if by == "binomial":
            taxinfo = taxinfo[0]

        if taxinfo["submittable"] != 'true':
            errors.append("TAXON_ID " + taxon + " is not submittable to ENA")

        if taxinfo["rank"] not in ["species", "subspecies"]:
            errors.append("TAXON_ID " + taxon + " is not a 'species' or 'subspecies' level entity.")

        is_taxon_binomial = d_utils.convertStringToBoolean(taxinfo["binomial"])
        if is_binomial_required and not is_taxon_binomial:
            errors.append(MESSAGE['validation_msg_invalid_binomial_name'] % (taxon, taxinfo["scientificName"]))
    except requests.exceptions.Timeout:
        l.error("ENA taxonomy lookup timed out for %s (url=%s)" % (taxon, url))
        errors.append(
            "ENA taxonomy lookup timed out for " + taxon + ". The ENA service may be slow or unreachable; please retry.")
    except Exception as e:
        l.exception(e)
        try:
            errors.append(
                "ENA returned - " + (taxinfo.get("error", "no error returned") if taxinfo else (body or "no response")) + " - for TAXON_ID " + taxon)
        except (NameError, AttributeError):
            errors.append(MESSAGE['validation_msg_not_submittable_taxon'] % (taxon))

    return errors, taxinfo


def checkOntologyTerm(ontology_id, ancestor, term):
    """
    Check that `term` exists in the given ontology and descends from `ancestor`.

    Queries the EBI OLS4 API for an exact label/synonym match, then verifies
    that the matched entity has `ancestor` in its hierarchical ancestor list.

    Args:
        ontology_id: OLS ontology ID, e.g. "efo" or "uberon".
        ancestor:    Numeric part of the ancestor term ID, e.g. "0000001".
        term:        The label or synonym string to look up.

    Returns:
        True if a matching term that descends from ancestor is found, else False.
    """
    url = f"https://www.ebi.ac.uk/ols4/api/v2/entities?search={term}&size=10&page=0&facetFields=ontologyId+type&lang=en&exactMatch=true&ontologyId={ontology_id}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        for elm in data.get("elements",[]):
            # Accept a match on label or any synonym (handles both plain strings and
            # dicts with a "value" key, as OLS4 returns synonyms in both formats).
            if term in elm.get("label",[]) or any( isinstance(synonym, str) and synonym == term  or synonym.get("value") == term for synonym in elm.get("synonym",[])) :
                for ancestor_uri in elm.get("hierarchicalAncestor",[]):
                    # Ancestor URIs end with "{ontology_id}_{ancestor}" (case variants exist).
                    if ancestor_uri.endswith(f"{ontology_id}_{ancestor}") or ancestor_uri.endswith(f"{ontology_id.upper()}_{ancestor}"):
                        return True
    return False

def checkNCBITaxonTerm(term):
    """
    Verify that an NCBI Taxon ID exists and is not obsolete via EBI OLS4.

    Args:
        term: Numeric NCBI taxon ID as a string, e.g. "9606".

    Returns:
        True if the taxon is found and its CURIE matches "NCBITaxon:{term}",
        False otherwise.
    """
    url = f"https://www.ebi.ac.uk/ols4/api/v2/ontologies/ncbitaxon/classes/http%253A%252F%252Fpurl.obolibrary.org%252Fobo%252FNCBITaxon_{term}?includeObsoleteEntities=false"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        curie = data.get("curie","")
        if curie == f"NCBITaxon:{term}":
            return True
    return False

def clean_str(s):
    # Normalise Unicode spaces (e.g. NBSP → normal space)
    normalised = unicodedata.normalize('NFKC', str(s))
    # Remove all leading/trailing whitespace (including NBSP, zero-width)
    return normalised.strip()
