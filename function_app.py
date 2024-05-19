import azure.functions as func
import datetime
import json
import logging
from ingest_club_link import bp
from additional_functions_two import bp_second

app = func.FunctionApp()

app.register_blueprint(bp)
app.register_blueprint(bp_second)

#@app.function_name('FirstHTTPFunction')
#@app.route(route="myroute", auth_level=func.AuthLevel.ANONYMOUS)
#def test_function(req: func.HttpRequest) -> func.HttpResponse:
#    logging.info('Python HTTP trigger function processed a request')
#    return func.HttpResponse(
#        "wow this HTTP works",
#        status_code=200
#    )


#@app.function_name('SecondHTTPFunction')
#@app.route(route="newroute", auth_level=func.AuthLevel.ANONYMOUS)
#def test_function(req: func.HttpRequest) -> func.HttpResponse:
#    logging.info('Python HTTP trigger function processed second request')
#    return func.HttpResponse(
#        "wow second HTTP works",
#        status_code=200
#    )