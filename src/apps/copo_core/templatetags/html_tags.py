from django import template
from allauth.socialaccount import providers

register = template.Library()


@register.simple_tag
def get_providers_orcid_first():
    """
    Returns a list of social authentication providers with Orcid as the first entry

    Usage: `{% get_providers_orcid_first as socialaccount_providers %}`.

    Then within the template context, `socialaccount_providers` will hold
    a list of social providers configured for the current site.
    """
    p_list = providers.registry.get_class_list()
    for idx, p in enumerate(p_list):
        if p.id == 'orcid':
            o = p_list.pop(idx)
    result = [o] + p_list
    return [{"id": o.id, "name": o.name} for o in result]
