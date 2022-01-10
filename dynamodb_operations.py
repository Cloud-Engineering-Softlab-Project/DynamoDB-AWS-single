import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
from pprint import pprint
from decimal import Decimal
import json

dynamodb = boto3.resource('dynamodb', region_name='eu-west-3')

# Create table
def create_movie_table():

    table = dynamodb.create_table(
        TableName='Movies',
        KeySchema=[
            {
                'AttributeName': 'year',
                'KeyType': 'HASH'  # Partition key
            },
            {
                'AttributeName': 'title',
                'KeyType': 'RANGE'  # Sort key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'year',
                'AttributeType': 'N'
            },
            {
                'AttributeName': 'title',
                'AttributeType': 'S'
            },

        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )
    return table

'''
movie_table = create_movie_table()
print("Table status:", movie_table.table_status)
'''


# Load items
def load_movies(movies):

    table = dynamodb.Table('Movies')
    for movie in movies:
        year = int(movie['year'])
        title = movie['title']
        print("Adding movie:", year, title)
        table.put_item(Item=movie)

# with open('../data_files/moviedata.json') as json_file:
#    movie_list = json.load(json_file, parse_float=Decimal)
# load_movies(movie_list)


# Add an item
def put_movie(title, year, plot, rating):

    table = dynamodb.Table('Movies')
    response = table.put_item(
       Item={
            'year': year,
            'title': title,
            'info': {
                'plot': plot,
                'rating': rating
            }
        }
    )
    return response

'''
movie_resp = put_movie("The Big New Movie", 2015,
                           "Nothing happens at all.", 0)
print("Put movie succeeded:")
pprint(movie_resp, sort_dicts=False)
'''


# Read an item
def get_movie(title, year):

    table = dynamodb.Table('Movies')

    try:
        response = table.get_item(Key={'year': year, 'title': title})
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        return response['Item']

'''
movie = get_movie("The Big New Movie", 2015)
if movie:
    print("Get movie succeeded:")
    pprint(movie, sort_dicts=False)
'''


# Update an item
def update_movie(title, year, rating, plot, actors):

    table = dynamodb.Table('Movies')

    response = table.update_item(
        Key={
            'year': year,
            'title': title
        },
        UpdateExpression="set info.rating=:r, info.plot=:p, info.actors=:a",
        ExpressionAttributeValues={
            ':r': Decimal(rating),
            ':p': plot,
            ':a': actors
        },
        ReturnValues="UPDATED_NEW"  # The ReturnValues parameter instructs DynamoDB to return only the updated attributes (UPDATED_NEW).
    )
    return response

'''
update_response = update_movie(
        "The Big New Movie", 2015, 5.5, "Everything happens all at once.",
        ["Larry", "Moe", "Curly"])
print("Update movie succeeded:")
pprint(update_response, sort_dicts=False)
'''


# Increment an Atomic Counter
def increase_rating(title, year, rating_increase):

    table = dynamodb.Table('Movies')

    response = table.update_item(
        Key={
            'year': year,
            'title': title
        },
        UpdateExpression="set info.rating = info.rating + :val",
        ExpressionAttributeValues={
            ':val': Decimal(rating_increase)
        },
        ReturnValues="UPDATED_NEW"
    )
    return response

'''
update_response = increase_rating("The Big New Movie", 2015, 1)
print("Update movie succeeded:")
pprint(update_response, sort_dicts=False)
'''


# Update an Item (Conditionally)
def remove_actors(title, year, actor_count):

    table = dynamodb.Table('Movies')

    try:
        response = table.update_item(
            Key={
                'year': year,
                'title': title
            },
            UpdateExpression="remove info.actors[0]",
            ConditionExpression="size(info.actors) > :num",
            ExpressionAttributeValues={':num': actor_count},
            ReturnValues="UPDATED_NEW"
        )
    except ClientError as e:
        if e.response['Error']['Code'] == "ConditionalCheckFailedException":
            print(e.response['Error']['Message'])
        else:
            raise
    else:
        return response

'''
print("Attempting conditional update (expecting failure)...")
update_response = remove_actors("The Big New Movie", 2015, 3)
if update_response:
    print("Update movie succeeded:")
    pprint(update_response, sort_dicts=False)
'''


# Delete an Item (Conditionally)
def delete_underrated_movie(title, year, rating):

    table = dynamodb.Table('Movies')

    try:
        response = table.delete_item(
            Key={
                'year': year,
                'title': title
            },
            ConditionExpression="info.rating <= :val",
            ExpressionAttributeValues={
                ":val": Decimal(rating)
            }
        )

        # Without condition
        '''
        response = table.delete_item(
            Key={
                'year': year,
                'title': title
            }
        ) 
        '''

    except ClientError as e:
        if e.response['Error']['Code'] == "ConditionalCheckFailedException":
            print(e.response['Error']['Message'])
        else:
            raise
    else:
        return response

'''
print("Attempting a conditional delete...")
delete_response = delete_underrated_movie("The Big New Movie", 2015, 5)
if delete_response:
    print("Delete movie succeeded:")
    pprint(delete_response, sort_dicts=False)
'''


# Query items
def query_movies(year):

    table = dynamodb.Table('Movies')
    response = table.query(
        KeyConditionExpression=Key('year').eq(year)
    )
    return response['Items']

'''
query_year = 1985
print(f"Movies from {query_year}")
movies = query_movies(query_year)
for movie in movies:
    print(movie['year'], ":", movie['title'])
'''


# Query more items
def query_and_project_movies(year, title_range):

    table = dynamodb.Table('Movies')
    print(f"Get year, title, genres, and lead actor")

    # Expression attribute names can only reference items in the projection expression.
    response = table.query(
        ProjectionExpression="#yr, title, info.genres, info.actors[0]",
        ExpressionAttributeNames={"#yr": "year"},
        KeyConditionExpression=
            Key('year').eq(year) & Key('title').between(title_range[0], title_range[1])
    )
    return response['Items']

'''
query_year = 2013
query_range = ('A', 'Z')
print(f"Get movies from {query_year} with titles from "
      f"{query_range[0]} to {query_range[1]}")
movies = query_and_project_movies(query_year, query_range)
for movie in movies:
    print(f"\n{movie['year']} : {movie['title']}")
    pprint(movie['info'])
'''