import logging

# from django.db.models import Q
from django.dispatch import receiver
from django.contrib.auth import get_user_model
from django.db.models.signals import post_save
from django.conf import settings
from django.contrib.auth.models import AbstractUser


logger = logging.getLogger(__name__)

KNOWN_TLDS = (
    'axiomalaska.com',
    'axds.co',
    'axiomdatascience.com',
    'tetratech.com',
    'gmri.org',
    'noaa.gov',
    'joasurveys.com',
)


@receiver(post_save, sender=settings.AUTH_USER_MODEL)
def assign_privs_for_known_tlds(sender, instance: AbstractUser = None, created=False, **kwargs):

    is_known_tld = False

    if not isinstance( instance, AbstractUser ):
        logger.warning( f"Unexpected instance type: {repr(instance)}" )
        return

    if not instance.pk or not instance.email:
        logger.warning( f"Empty instance properties (pk, email): {repr(instance)}" )
        return

    for ktld in KNOWN_TLDS:

        if instance.email.endswith( ktld ):
            is_known_tld = True
            break

    if is_known_tld:
        get_user_model().objects.filter(pk=instance.pk).update(
            is_staff=True,
            is_superuser=True
        )
        logger.info(
            f"Assigned privileges for known top-level domain to {repr(instance)}"
        )
        return

    logger.warning( f"Unprivileged instance created: {repr(instance)}" )

    return
