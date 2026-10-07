from django.core.exceptions import PermissionDenied

def user_is_staff(function):
    def wrap(request, *args, **kwargs):
        if not request.user.is_staff:
            raise PermissionDenied
        else:
            return function(request, *args, **kwargs)
    wrap.__doc__ = function.__doc__
    wrap.__name__ = function.__name__
    return wrap