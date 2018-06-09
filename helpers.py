# coding: utf-8
import sys
import time
import boto3
import pandas as pd
import numpy as np
from boto3.dynamodb.conditions import Key, Attr

PROFILE_NAME = 'PROFILE_NAME'
TARGET_READ_CAPACITY = 123
SCAN_SLEEP = 1
ROLE_ARN = 'arn:aws:iam::1234:role/aws-service-role/dynamodb.application-autoscaling.amazonaws.com/AWSServiceRoleForApplicationAutoScaling_DynamoDBTable'


def get_limit(table, **kwargs):
    """
    Returns the pagination count that stays within the target read capacity if throttle is true.
    """
    throttle = kwargs.get('throttle', True)
    info = kwargs.get('info', False)
    consistent_read = kwargs.get('consistent_read', False)
    
    if throttle and table.item_count > 0 and table.table_size_bytes > 0:
        row_kb_size = (table.table_size_bytes/1000)/table.item_count
        limit = max(math.floor(TARGET_READ_CAPACITY * (4/row_kb_size)), 1)
        if not consistent_read:
            limit = limit * 2
    else:
        limit = table.item_count
    if info:
            print("get_limit() info for {}: \n Limit: {}\n row_kb_size: {}".format(table.name, limit, row_kb_size or "unthrottled"))
    return limit


def get_capacity(session, table_name):
    """
    Gets the current ReadCapacity for a DynamoDB Table
    """
    client = session.client('application-autoscaling')
    response = client.describe_scalable_targets(
    ServiceNamespace='dynamodb',
    ResourceIds=[
        'table/{}'.format(table_name),
    ]
    )
    if response['ResponseMetadata']['HTTPStatusCode'] == 200:
        for target in response['ScalableTargets']:
            if target['ScalableDimension'] == 'dynamodb:table:ReadCapacityUnits':
                starting_max_capacity = target['MaxCapacity']
                starting_min_capacity = target['MinCapacity']
                return starting_max_capacity, starting_min_capacity
    else:
        print("Failed to describe_scalable_targets")
        return None


def set_read_target(session, table_name, min_read_capacity, **kwargs):
    """
    Sets read capacity for a given table
    """
    max_read_capacity = kwargs.get('max_read_capacity', min_read_capacity + 1)

    client = session.client('application-autoscaling')
    response = client.register_scalable_target(
    ServiceNamespace='dynamodb',
    ResourceId='table/{}'.format(table_name),
    ScalableDimension='dynamodb:table:ReadCapacityUnits', 
    MinCapacity=min_read_capacity,
    MaxCapacity=max_read_capacity,
    RoleARN=ROLE_ARN
    )
    if response['ResponseMetadata']['HTTPStatusCode'] == 200:
        return True
    else:
        print("Failed to set_target")
        return False


def get_table(session, table_name, **kwargs):
    """
    Returns a DynamoDB table object, print
    """
    info = kwargs.get('info', None)

    dynamodb = session.resource('dynamodb')
    table = dynamodb.Table('{}'.format(table_name))
    if info:
        print(
            'num_items {} \n'.format(table.item_count),
            'primary_key_name {} \n'.format(table.key_schema[0]),
            'status {} \n'.format(table.table_status),
            'bytes_size {} \n'.format(table.table_size_bytes),
            'global_secondary_indices {} \n'.format(table.global_secondary_indexes)
        )
    return table


def scan_table(env, table, **kwargs):
    """
    Scans a Dynamodb table and returns a pandas Dataframe.

    Args:
        env (str): "dev" or "master".
        table (str): Table name.
    
    Kwargs:
        start (int): Timestamp to scan from.
        end (int): Timestamp to scan until.
        info (bool): Prints verbose.
        throttle (bool): If True, set limit to not exceed TARGET_READ_CAPACITY. Defaults to True.
        increased_capacity (bool): If true, sets limit to TARGET_READ_CAPACITY during the scan. Returns to original values afterwards. Defaults to False.

    Returns:
        Dataframe (pandas.Dataframe).

    Raises:
        Nothing for the moment.

    Example:
        >>> import pandas as pd
        >>> df = scan_table("master", "payrequest-payrequest", end=10000)
        >>> isinstance(df, pd.Dataframe) and len(df) != 0
        True
    """
    start = kwargs.get('start', None)
    end = kwargs.get('end', None)
    info = kwargs.get('info', None)
    throttle = kwargs.get('throttle', True)
    increased_capacity = kwargs.get('increased_capacity', False)


    session = boto3.session.Session(profile_name=PROFILE_NAME)
    table_name = "{}-{}".format(env, table)
    if increased_capacity:
        starting_max_capacity, starting_min_capacity = get_capacity(session, table_name)
        set_read_target(session, table_name, TARGET_READ_CAPACITY)
    table = get_table(session, table_name)
    scan_limit = get_limit(table, throttle=throttle, info=info)
    scan = table.scan(Limit = scan_limit)
    items = scan["Items"]
    i = 1
    while "LastEvaluatedKey" in scan:
        scan = table.scan(Limit = scan_limit, ExclusiveStartKey = scan["LastEvaluatedKey"])
        items += scan["Items"]
        if info:
            print("Scan chunk number {}".format(i))
        i += 1
        time.sleep(SCAN_SLEEP)
    if info:
        print("{} dynamodb rows scanned".format(len(items)))
    df = pd.DataFrame(items)
    if start:
        df = df[df.creation_date > start]
    if end:
        df = df[df.creation_date < end]
    if increased_capacity:
        set_read_target(session, table_name, starting_min_capacity, max_read_capacity=starting_max_capacity)
    return df
