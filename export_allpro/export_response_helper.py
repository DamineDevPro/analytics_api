from rest_framework import status
from django.http import JsonResponse


class ExportResponses:

    def get_status_401(self, message="Unauthorized"):
        response = {"message": message}
        return JsonResponse(response, safe=False, status=status.HTTP_401_UNAUTHORIZED)

    def get_status_400(self, response):
        return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

    def get_status_200(self, data):
        response = {"message": "success", "data": data}
        return JsonResponse(response, safe=False, status=status.HTTP_200_OK)

    def get_status_204(self):
        response = {"message": "No Data Found"}
        return JsonResponse(response, safe=False, status=status.HTTP_404_NOT_FOUND)

    def get_status_500(self, ex):
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        finalResponse = {"message": "Error", "data": []}
        return JsonResponse(finalResponse, safe=False, status=500)

    def get_status_422(self, response):
        return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)

    def get_status_404(self, message):
        response = {"message": message}
        return JsonResponse(response, safe=False, status=status.HTTP_404_NOT_FOUND)
