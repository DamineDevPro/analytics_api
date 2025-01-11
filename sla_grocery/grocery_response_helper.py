from rest_framework import status
from django.http import JsonResponse

class GroceryResponses:

    def get_status_401():
        response = {"message": "Unauthorized"}
        return JsonResponse(response, safe=False, status=status.HTTP_401_UNAUTHORIZED)

    def get_status_400(params):
        response = {"message": "mandatory field missing", "missing_params": params}
        return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

    def get_status_200(data, pen_count=0):
        response = {"message": "success", "data": data}
        if pen_count: response["pen_count"] = pen_count
        return JsonResponse(response, safe=False, status=status.HTTP_200_OK)

    def get_status_404():
        response = {"message": "Given storeId is not a DC(Distribution Center)"}
        return JsonResponse(response, safe=False, status=status.HTTP_404_NOT_FOUND)

    def get_status_204():
        response = {"message": "No Data Found"}
        return JsonResponse(response, safe=False, status=status.HTTP_204_NO_CONTENT)

    def get_status_500(ex):
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        finalResponse = {"message": message, "data": []}
        return JsonResponse(finalResponse, safe=False, status=500)
