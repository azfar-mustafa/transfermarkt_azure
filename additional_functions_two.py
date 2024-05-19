import azure.functions as func
import datetime
import json
import logging

bp_second = func.Blueprint()

@bp_second.function_name('FirstHTTPFunction')
@bp_second.route(route="myroute", auth_level=func.AuthLevel.ANONYMOUS)
def test_function(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request')
    return func.HttpResponse(
        "wow this HTTP works",
        status_code=200
    )