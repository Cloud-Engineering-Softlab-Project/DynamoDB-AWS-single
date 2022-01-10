from flask import Flask
from flask_restx import Api
import json
import time
import boto3

global db, app, api, times

def measure_time(func):
    def wrap(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()

        times[func.__name__] = end - start
        # print(func.__name__, end - start)
        return result

    return wrap

def init():

    global times
    times = {}

    # Initialize firestore
    global db
    db = boto3.resource('dynamodb', region_name='eu-west-3')

    # Initialize Flask App
    global app, api
    app = Flask(__name__)
    api = Api(app=app, version="1.0", title="Softlab-Project DynamoDB APIs")

    return app
