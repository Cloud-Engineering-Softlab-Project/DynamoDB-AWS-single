import json
from datetime import datetime
import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
from decimal import Decimal

dynamodb = boto3.resource('dynamodb', region_name='eu-west-3')

def fill_zones():

    # Read JSON file
    with open('../data_files/entsoeAreaRef_202111071647.json') as f:
        reference_zones = json.load(f)['entsoeAreaRef']

    table = dynamodb.Table('ReferenceZones')
    for zone in reference_zones:

        Id = zone['Id']
        country_FK = zone['Country_FK']
        print("Adding zone:", Id, country_FK)
        table.put_item(Item=zone)
0
def fill_data():

    # Read JSON file
    with open('../data_files/ActualTotalLoad_202111071723.json') as f:
        energy_data = json.load(f)['data']

    table = dynamodb.Table('TotalLoadData')
    for item in energy_data:

        # Convert float values to Decimal
        item['TotalLoadValue'] = Decimal(str(item['TotalLoadValue']))

        print(item['Id'])
        table.put_item(Item=item)

# Fill Resolution Codes collection
def fill_codes():

    # Read JSON file
    with open('../data_files/ResolutionCode_202111071645.json') as f:
        resolution_codes = json.load(f)['ResolutionCode']

    table = dynamodb.Table('ResolutionCodes')
    for code in resolution_codes:

        table.put_item(Item=code)

def create_zones_table():
    table = dynamodb.create_table(
        TableName='ReferenceZones',
        KeySchema=[
            {
                'AttributeName': 'Id',  # Combination of 2 attrs 'Id'
                'KeyType': 'HASH'  # Partition key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'Id',
                'AttributeType': 'N'
            },
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 1,
            'WriteCapacityUnits': 1
        }
    )

    return table

def create_energy_table():
    table = dynamodb.create_table(
        TableName='TotalLoadData',
        KeySchema=[
            {
                'AttributeName': 'entsoeAreaReference_FK',
                'KeyType': 'HASH'  # Partition key
            },
            {
                'AttributeName': 'DateTime',
                'KeyType': 'RANGE'  # Sort key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'entsoeAreaReference_FK',
                'AttributeType': 'N'
            },
            {
                'AttributeName': 'DateTime',
                'AttributeType': 'S'
            },

        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 1,
            'WriteCapacityUnits': 1
        }
    )

    return table

create_energy_table()

def create_codes_table():
    table = dynamodb.create_table(
        TableName='ResolutionCodes',
        KeySchema=[
            {
                'AttributeName': 'Id',
                'KeyType': 'HASH'  # Partition key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'Id',
                'AttributeType': 'N'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 1,
            'WriteCapacityUnits': 1
        }
    )

    return table

# zones_table = create_codes_table()
# print("Table status:", zones_table.table_status)

# Get item based on zone_id
def get_zone_item(zone_id):

    table = dynamodb.Table('ReferenceZones')
    print(f"Get Id_CountryFK, AreaRefAddedOn, Id, and Country_FK")

    # Expression attribute names can only reference items in the projection expression.
    response = table.query(
        KeyConditionExpression=Key('Id_CountryFK').begins_with(f"{zone_id}_")
    )
    return response['Items']

#zones = query_ref_zones(('2020-10-13 00:00:00.384259000', '2020-10-20 00:00:00.384259000'))
#for zone in zones:
#    print(zone)