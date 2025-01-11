from rest_framework import status
from django.http import JsonResponse


class RideResponses:

    def get_status_401(self):
        response = {"message": "Unauthorized"}
        return JsonResponse(response, safe=False, status=status.HTTP_401_UNAUTHORIZED)

    def get_status_400(self, support=None, params=None, message="mandatory field missing"):
        if params is None: params = []
        if support is None: support = {}
        response = {"message": message, "missing_params": params}
        if support:response["support"] = support
        return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

    def get_status_200(self, response):
        return JsonResponse(response, safe=False, status=status.HTTP_200_OK)

    def get_status_204(self):
        response = {"message": "No Data Found"}
        return JsonResponse(response, safe=False, status=status.HTTP_204_NO_CONTENT)

    def get_status_500(self, ex):
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        finalResponse = {"message": message, "data": []}
        return JsonResponse(finalResponse, safe=False, status=500)

    def get_status_404(self, message):
        response = {"message": message}
        return JsonResponse(response, safe=False, status=status.HTTP_404_NOT_FOUND)

    def get_status_422(self, message):
        response = {"message": message}
        return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)

    def get_status(self, message, status):
        return JsonResponse(message, safe=False, status=status)
