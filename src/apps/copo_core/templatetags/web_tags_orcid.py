from django import template

register = template.Library()


@register.filter(name="caps")
def caps(value):
    return value.capitalize()


@register.filter(name="subtitle")
def subtitle(value):
    return ', ' + value


@register.filter(name="add_break")
def add_br(value):
    return "<br/>" + value


@register.filter(name="add_uri_break")
def add_uri_break(value):
    if value is not None:
        return '<br/><strong>URI: </strong><a href="' + value + '">' + value + '</a>'


@register.filter(name="date_present_year")
def date_present(value):
    if value == '':
        return 'Present'
    else:
        return value


@register.filter(name="date_present_month")
def date_present_month(value):
    if value == '':
        return ''
    else:
        return ' - ' + value
