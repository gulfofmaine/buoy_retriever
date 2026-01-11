import re

from django.conf import settings
from django.middleware.csrf import CsrfViewMiddleware


class CustomCsrfMiddleware(CsrfViewMiddleware):
    """
    Custom CSRF middleware that exempts certain URL patterns from CSRF protection.
    """

    def process_request(self, request):
        # Check if the request path matches any exempt patterns
        if hasattr(settings, "CSRF_EXEMPT_URLS"):
            for pattern in settings.CSRF_EXEMPT_URLS:
                if re.match(pattern, request.path_info.lstrip("/")):
                    # Mark this request as exempt from CSRF
                    setattr(request, "_dont_enforce_csrf_checks", True)
                    return None
        return super().process_request(request)
