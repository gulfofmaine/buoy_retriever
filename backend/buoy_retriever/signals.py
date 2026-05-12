import logging

from django.dispatch import receiver
# from django.contrib.auth import get_user_model
from django.db.models.signals import post_save
from django.conf import settings
from django.contrib.auth.models import AbstractUser


logger = logging.getLogger(__name__)


@receiver(post_save, sender=settings.AUTH_USER_MODEL)
def assign_privs_for_new_user(sender, instance: AbstractUser = None, created=False, **kwargs):

    # TODO: Placeholder in case we want to use signals to assign any baseline
    # privileges to new users.

    # logger.warning( f"Updating default privileges for new user: {repr(instance)}" )

    # get_user_model().objects.filter(pk=instance.pk).update(
    #     is_staff=False,
    #     is_superuser=False
    # )

    return
