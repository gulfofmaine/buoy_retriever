from django.contrib.auth.models import AbstractUser
from django.urls import reverse
from allauth.account.adapter import DefaultAccountAdapter
from allauth.socialaccount.adapter import DefaultSocialAccountAdapter
from django.core.exceptions import ValidationError


class User(AbstractUser):
    pass

class UserAccountAdapter(DefaultAccountAdapter):
    pass
    # def is_open_for_signup(self, request):
    #     if request.path.rstrip("/") == reverse("account_signup").rstrip("/"):
    #         return False
    #     return True


class SocialAccountAdapter(DefaultSocialAccountAdapter):
    # Can not disconnect any social accounts
    def validate_disconnect(self, account, accounts):
        raise ValidationError("Can not disconnect")
