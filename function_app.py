import azure.functions as func
import logging

from ingest_player_data import bp

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)
app.register_functions(bp) # register the DF functions