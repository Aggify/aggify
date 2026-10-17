from django.http import JsonResponse
from django.urls import path

from aggify import Count
from aggify.contrib.django.shortcuts import aget_object_or_404, get_object_or_404
from tests.models import Account


async def async_detail(request, username):
    acc = await aget_object_or_404(Account, username=username)
    return JsonResponse({"username": acc.username, "age": acc.age})


def sync_detail(request, username):
    acc = get_object_or_404(Account, username=username)
    return JsonResponse({"username": acc.username, "age": acc.age})


async def async_stats(request):
    rows = await Account.objects.values("is_verified").annotate(n=Count()).order_by("is_verified").alist()
    return JsonResponse({"rows": rows})


async def async_create(request):
    obj = await Account.objects.acreate(username=request.GET["u"])
    return JsonResponse({"id": str(obj.pk)})


urlpatterns = [
    path("async/<str:username>/", async_detail),
    path("sync/<str:username>/", sync_detail),
    path("stats/", async_stats),
    path("create/", async_create),
]
