from common.schemas.utils.data_formats import DataFormats
from django.conf import settings
from django.contrib.auth.models import User
from django.http import HttpResponse
from django.shortcuts import redirect
from rest_framework.authtoken.views import ObtainAuthToken
from rest_framework.authtoken.models import Token
from rest_framework.response import Response
from rest_framework.authentication import SessionAuthentication

import json
import jsonpickle


def forward_to_swagger(request):
    response = redirect('/static/swagger/apidocs_index.html')

    return response


def check_orcid_credentials(request):
    # TODO - here we check if the orcid tokens are valid
    out = {
        'exists': False,
        'authorise_url': settings['REPOSITORIES']['ORCID']['urls']['authorise_url'],
    }
    return HttpResponse(jsonpickle.encode(out))


# call only if you want to generate a new template
def generate_ena_template(request):
    temp_dict = DataFormats("ENA").generate_ui_template()
    return HttpResponse(jsonpickle.encode(temp_dict))


def numbers(request):
    profiles = number_of_profiles()
    samples = number_of_samples()
    users = number_of_users()
    datafiles = number_of_datafiles()
    out = {
        "profiles": profiles,
        "samples": samples,
        "users": users,
        "datafiles": datafiles,
    }
    return HttpResponse(json.dumps(out))


def number_of_profiles():
    from common.dal.profile_da import Profile

    return Profile().get_number()


def number_of_samples():
    from common.dal.sample_da import Sample

    # get total number of sample records in COPO instance
    return Sample().get_number()


def number_of_users():
    return User.objects.all().count()


def number_of_datafiles():
    from common.dal.copo_da import DataFile

    return DataFile().get_number()


class CustomAuthToken(ObtainAuthToken):

    def post(self, request, *args, **kwargs):
        serializer = self.serializer_class(
            data=request.data, context={'request': request}
        )
        serializer.is_valid(raise_exception=True)
        user = serializer.validated_data['user']
        token, created = Token.objects.get_or_create(user=user)
        return Response(
            {'token': "Token " + token.key, 'user_id': user.pk, 'email': user.email}
        )


class CsrfExemptSessionAuthentication(SessionAuthentication):

    def enforce_csrf(self, request):
        return  # To not perform the csrf check previously happening
