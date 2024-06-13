# """
# * File: src\pod_doc_upload\index.py
# * Project: Omni-datawarehouse-backend-services
# * Author: Bizcloud Experts
# * Date: 2023-11-16
# * Confidential and Proprietary
# """
import os
import base64
import logging
import boto3
import json
import requests
import io
from datetime import datetime, timedelta
from src.shared.amazonPODHelperFiles.cognito import AWSSRP, AWSIDP
from requests_aws4auth import AWS4Auth

sns = boto3.client('sns')
sqs = boto3.client('sqs')
dynamodb = boto3.client('dynamodb')

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

SHIPMENT_HEADER_TABLE_STREAM_QLQ = os.environ["SHIPMENT_HEADER_TABLE_STREAM_QLQ"]
SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]
SHIPPEO_USERNAME = os.environ["SHIPPEO_USERNAME"]
SHIPPEO_PASSWORD = os.environ["SHIPPEO_PASSWORD"]
SHIPPEO_UPLOAD_DOC_URL = os.environ["SHIPPEO_UPLOAD_DOC_URL"]
LOG_TABLE = os.environ["LOG_TABLE"]
SHIPPEO_GET_TOKEN_URL = os.environ["SHIPPEO_GET_TOKEN_URL"]
WT_WEBSLI_API_URL = os.environ["WT_WEBSLI_API_URL"]
TOKEN_EXPIRATION_DAYS = os.environ["TOKEN_EXPIRATION_DAYS"]
USERNAME = os.environ["AMAZON_USER_NAME"]
PASSWORD = os.environ["AMAZON_PASSWORD"]
TRANSACTION_TABLE = os.environ["TRANSACTION_TABLE"]
REFERENCE_TABLE_ORDER_NO_INDEX = os.environ["REFERENCE_TABLE_ORDER_NO_INDEX"]
REFERENCE_TABLE = os.environ["REFERENCE_TABLE"]
ENVIRONMENT = os.environ["STAGE"]
SHIPPEO_POD_DOC_UPLOAD_WEBSLI_TOKEN = os.environ["SHIPPEO_POD_DOC_UPLOAD_WEBSLI_TOKEN"]
AMAZON_POD_DOC_UPLOAD_WEBSLI_TOKEN = os.environ["AMAZON_POD_DOC_UPLOAD_WEBSLI_TOKEN"]

HOST = os.environ["HRPSL_HOST"]
STAGE = os.environ["HRPSL_STAGE"]
ENDPOINT = f'https://{HOST}/{STAGE}/requestShipmentAttachmentUrl'
REGION = os.environ["HRPSL_REGION"]
SERVICE = os.environ["HRPSL_SERVICE"]
COGNITO_CLIENT_ID = os.environ["COGNITO_CLIENT_ID"]
COGNITO_USER_POOL_ID = os.environ["COGNITO_USER_POOL_ID"]
COGNITO_IDENTITY_POOL_ID = os.environ["COGNITO_IDENTITY_POOL_ID"]
COGNITO_REGION = os.environ["COGNITO_REGION"]
COGNITO_PROVIDER = f'cognito-idp.{COGNITO_REGION}.amazonaws.com/{COGNITO_USER_POOL_ID}'


def handler(event, context):
    body = ""
    order_no = ""
    housebill_no = ""
    try:
        LOGGER.info("context: %s", context)
        LOGGER.info("Event: %s", event)
        record = event['Records'][0]
        body = json.loads(record['body'])
        order_no = body['Item']['PK_OrderNo']
        housebill_no = body['Item']['Housebill']
        user_id = body['Item']['UserId']
        client = body['Client']
        file_header_table_data = body['FileHeaderTableData']
        LOGGER.info("client: %s", client)
        LOGGER.info("file_header_table_data: %s", file_header_table_data)

        # Commented the shippeo, will have to uncomment this when we push shippeo into production.
        # if client == 'shippeo':
        #     process_shippeo(order_no, housebill_no, file_header_table_data)

        if client == 'amazon':
            process_amazon(order_no, housebill_no,
                           user_id, file_header_table_data)

        update_transaction_table(order_no, housebill_no, 'SUCCESS')
    except Exception as e:
        LOGGER.error(f"Error: {e}")
        try:
            update_transaction_table(order_no, housebill_no, 'FAILED')
            insert_log(housebill_no, body, str(e))
            sqs.send_message(
                QueueUrl=SHIPMENT_HEADER_TABLE_STREAM_QLQ,
                MessageBody=json.dumps(body)
            )
            if ENVIRONMENT == 'prod':
                sns.publish(
                    TopicArn=SNS_TOPIC_ARN,
                    Subject="Error on pod-upload-doc lambda",
                    Message=f"Error in pod-upload-doc lambda: {e}"
                )
        except Exception as e:
            LOGGER.error(f"Error sending error notifications: {e}")


def process_shippeo(order_no, housebill_no, file_header_table_data):
    try:
        existing_token = get_existing_token()

        if existing_token == None:
            # Get token
            basic_auth = get_basic_auth(SHIPPEO_USERNAME, SHIPPEO_PASSWORD)
            LOGGER.info("basic_auth: %s", basic_auth)
            token_response = requests.post(
                SHIPPEO_GET_TOKEN_URL,
                headers={'Authorization': f'{basic_auth}'}
            )
            token = token_response.json()['data']['token']
            existing_token = token
            insert_token(token)

        final_token = existing_token
        # for record in records:
        doc_type = file_header_table_data['FK_DocType']
        LOGGER.info("doc_type: %s", doc_type)

        if doc_type == None:
            LOGGER.info(
                f"House bill : {housebill_no} with order no: {order_no} is not valid")
            return {
                'statusCode': 200
            }

        upload_to_url = f"{SHIPPEO_UPLOAD_DOC_URL}/{housebill_no}/files"
        # Upload docs
        result = upload_docs(upload_to_url, final_token,
                             housebill_no, doc_type)
        if result['status'] == 200:
            insert_log(housebill_no, result['data'])
            update_transaction_table(order_no, housebill_no, 'SUCCESS')
        return {
            'statusCode': 200
        }
    except Exception as e:
        LOGGER.error(f"process shippeo error: {e}")
        raise e


def process_amazon(order_no, housebill_no, user_id, shipment_file_data):
    try:
        b64_data = ""
        doc_type = shipment_file_data['FK_DocType']
        b64str_response = call_wt_rest_api(
            housebill_no, AMAZON_POD_DOC_UPLOAD_WEBSLI_TOKEN, doc_type)
        if b64str_response == 'error':
            LOGGER.info('Error calling WT REST API')
        else:
            b64_data = b64str_response['b64str']

        if shipment_file_data == None:
            LOGGER.info(
                f"House bill : {housebill_no} with order no: {order_no} is not valid")
            return f"House bill : {housebill_no} with order no: {order_no} is not valid"

        reference_no = get_data_from_reference_table(order_no)
        LOGGER.info("reference_no: %s", reference_no)

        pro_number = reference_no if reference_no != None else housebill_no
        filename = shipment_file_data['FileName']
        file_type = 'POD'
        description = 'HCPOD'
        file_extension = filename.split(
            '.')[-1].lower() if isinstance(filename.split('.')[-1], str) else '.pdf'
        mime_type = 'application/pdf' if file_extension == 'pdf' else 'image/jpeg'
        request_source = 'OMNG'
        location_id = 'US'

        payload = {
            "proNumber": pro_number,
            "filename": filename,
            "type": file_type,
            "description": description,
            "mimeType": mime_type,
            "requestSource": request_source,
            "locationId": location_id,
            "userId": user_id
        }
        LOGGER.info("payload: %s", payload)
        LOGGER.info("USERNAME: %s", USERNAME)
        LOGGER.info("COGNITO_USER_POOL_ID: %s", COGNITO_USER_POOL_ID)
        LOGGER.info("COGNITO_IDENTITY_POOL_ID: %s", COGNITO_IDENTITY_POOL_ID)
        LOGGER.info("COGNITO_CLIENT_ID: %s", COGNITO_CLIENT_ID)
        LOGGER.info("COGNITO_PROVIDER: %s", COGNITO_PROVIDER)
        LOGGER.info("COGNITO_REGION: %s", COGNITO_REGION)
        LOGGER.info("SERVICE: %s", SERVICE)
        LOGGER.info("REGION: %s", REGION)

        cognito_auth = CognitoAuth(username=USERNAME, password=PASSWORD, pool_id=COGNITO_USER_POOL_ID, identity_pool_id=COGNITO_IDENTITY_POOL_ID,
                                   client_id=COGNITO_CLIENT_ID, provider=COGNITO_PROVIDER, pool_region=COGNITO_REGION,
                                   service=SERVICE, service_region=REGION)
        auth = cognito_auth.get_auth()

        LOGGER.info(
            f'Endpoint: {ENDPOINT}, Host: {HOST}, region: {REGION}, service: {SERVICE}')

        response = requests.post(
            ENDPOINT,
            json=payload,
            auth=auth
        )
        LOGGER.info(f"{response.status_code} Reason: {response.reason}")

        if response.ok:
            url_json = json.loads(response.text)
            LOGGER.info(f"PreSigned URL: {url_json['url']}")
            upload_file_to_s3(url_json['url'], b64_data)
            insert_log(housebill_no, "File uploaded to s3 successfully.")
            return {"statusCode": 200, "data": "File uploaded to s3 successfully."}
        else:
            raise requests.exceptions.HTTPError(
                f"{response.status_code} Error: {response.reason} for url: {response.url} -- {response.text}")
    except Exception as e:
        LOGGER.error(f"process amazon error: {e}")
        raise e


def upload_file_to_s3(presigned_url, file_path):
    try:
        headers = {"x-amz-server-side-encryption": "AES256",
                   "Content-Type": "application/octet-stream"}
        while len(file_path) % 4 != 0:
            file_path += '='
        file_data = io.BytesIO(base64.b64decode(file_path))
        response = requests.put(presigned_url, headers=headers, data=file_data)
        if response.status_code == 200:
            LOGGER.info("Response Content: %s", response.content)
            LOGGER.info("File uploaded successfully.")
        else:
            LOGGER.info(
                f"Failed to upload file. Status code: {response.status_code}")
            LOGGER.info(
                f"Failed to upload file. Status code: {response.text}")
    except Exception as e:
        LOGGER.error(f"Upload document error: {e}")
        raise e


def get_data_from_reference_table(order_no):
    params = {
        'TableName': REFERENCE_TABLE,
        'IndexName': REFERENCE_TABLE_ORDER_NO_INDEX,
        'KeyConditionExpression': "FK_OrderNo = :FK_OrderNo",
        'FilterExpression': "(FK_RefTypeId = :FK_RefTypeId)",
        'ExpressionAttributeValues': {
            ":FK_OrderNo": {'S': order_no},
            ":FK_RefTypeId": {'S': "OD#"}
        },
    }
    LOGGER.info(f"response: {params}")
    try:
        # Define the query parameters

        # Query the DynamoDB table
        response = dynamodb.query(**params)
        LOGGER.info(f"response: {response}")
        # Check if there are items in the result
        items = response['Items']
        if len(items) > 0:
            return items[0]['ReferenceNo']['S']
        else:
            return None
    except Exception as e:
        LOGGER.info(f"Unable to query item. Error: {e}")
        raise e


def get_existing_token():
    try:
        # Define the query parameters
        params = {
            'TableName': LOG_TABLE,
            'KeyConditionExpression': "pKey = :pKey",
            'ExpressionAttributeValues': {
                ":pKey": {'S': 'token'}
            },
        }
        # Query the DynamoDB table
        response = dynamodb.query(**params)
        # Check if there are items in the result
        if len(response['Items']) > 0:
            return response['Items'][0]['data']['S']
        else:
            return None

    except Exception as e:
        LOGGER.info(f"Unable to insert item. Error: {e}")
        raise e


def insert_token(data):
    LOGGER.info(f"TOKEN_EXPIRATION_DAYS: {TOKEN_EXPIRATION_DAYS}")
    two_days_from_now = datetime.utcnow() + timedelta(days=float(TOKEN_EXPIRATION_DAYS))
    two_days_from_now_iso = two_days_from_now.isoformat()
    params = {
        'TableName': LOG_TABLE,
        'Item': {
            'pKey': {'S': 'token'},
            'data': {'S': data},
            'expiration': {'S': two_days_from_now_iso}
        }
    }

    try:
        dynamodb.put_item(**params)
        LOGGER.info("Token inserted successfully")
    except Exception as e:
        LOGGER.info(f"Unable to insert item. Error: {e}")
        raise e


def get_basic_auth(username, password):
    credentials = f"{username}:{password}"
    base64_credentials = base64.b64encode(
        credentials.encode('utf-8')).decode('utf-8')
    return f"Basic {base64_credentials}"


def upload_docs(upload_to_url, token, house_bill_no, doc_type):
    try:
        b64_data = ""
        file_name = ""
        b64str_response = call_wt_rest_api(
            house_bill_no, SHIPPEO_POD_DOC_UPLOAD_WEBSLI_TOKEN, doc_type)
        if b64str_response == 'error':
            LOGGER.info('Error calling WT REST API')
            # handle error case
        else:
            b64_data = b64str_response['b64str']
            file_name = b64str_response['file_name']

        file_data = io.BytesIO(base64.b64decode(b64_data))

        # Create a dictionary for the form data
        files = {'attachments[]': (
            file_name, file_data, 'application/octet-stream')}

        headers = {
            'Authorization': f'Bearer {token}',
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br'
        }

        response = requests.post(upload_to_url, headers=headers, files=files)

        # Print response data and status code
        LOGGER.info(f"Response Data: {response.text}")
        LOGGER.info(f"Status Code: {response.status_code}")
        if response.status_code == 200:
            return {'data': response.text, 'status': response.status_code}
        else:
            raise requests.HTTPError(
                f"Failed to upload file. Status code: {response.status_code}")
    except Exception as e:
        LOGGER.error(f"Upload document error: {e}")
        raise e


def call_wt_rest_api(housebill, websli_token, doc_type):
    try:
        # url = f"https://websli.omnilogistics.com/wtProd/getwtdoc/v1/json/{websli_token}/housebill={housebill}/doctype={doc_type}"
        url = f"{WT_WEBSLI_API_URL}/{websli_token}/housebill={housebill}/doctype={doc_type}"
        LOGGER.info(f"url: {url}")
        response = requests.get(url)

        if response.status_code == 200:
            data = response.json()
            b64str = data['wtDocs']['wtDoc'][0]['b64str']
            file_name = data['wtDocs']['wtDoc'][0]['filename']
            return {
                'b64str': b64str,
                'file_name': file_name
            }
        else:
            LOGGER.info(
                f"Error calling WT REST API for housebill {housebill}. Status Code: {response.status_code}")
            raise GetDocumentError(
                f"Error calling WT REST API for housebill {housebill}. Status Code: {response.status_code}")

    except Exception as e:
        LOGGER.info(
            f"Error calling WT REST API for housebill {housebill}:{e}")
        raise e


def insert_log(housebill_number, data, error=""):
    now = datetime.utcnow().isoformat()
    params = {
        'TableName': LOG_TABLE,
        'Item': {
            'pKey': {'S': housebill_number},
            'data': {'S': json.dumps(data)},
            'error': {'S': error},
            'lastUpdateTime': {'S': now},
        }
    }

    try:
        dynamodb.put_item(**params)
        LOGGER.info("Log inserted.")
    except Exception as e:
        LOGGER.info(f"Unable to store token. Error: {e}")
        raise e


def update_transaction_table(order_number, house_bill_number, status):
    try:
        now = datetime.utcnow().isoformat()
        key = {'orderNumber': {'S': order_number},
               'houseBillNumber': {'S': house_bill_number}}
        update_expression = 'SET #status = :status, #lastUpdateTime = :lastUpdateTime'
        expression_attribute_values = {
            ':status': {'S': status},
            ':lastUpdateTime': {'S': now},
        }
        expression_attribute_names = {
            '#status': 'status',
            '#lastUpdateTime': 'lastUpdateTime'
        }
        response = dynamodb.update_item(
            TableName=TRANSACTION_TABLE,
            Key=key,
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_attribute_values,
            ExpressionAttributeNames=expression_attribute_names,
            ReturnValues='UPDATED_NEW'
        )
        LOGGER.info(f"Transaction table update succeeded: {response}")
    except Exception as e:
        LOGGER.error(f"Error updating item: {e}")
        raise e


class GetDocumentError(Exception):
    pass


class WebsliTokenNotFoundError(Exception):
    pass


class HouseBillNotValidError(Exception):
    pass


class CognitoAuth(object):
    def __init__(self, username, password, pool_id, identity_pool_id, client_id, provider, pool_region, service, service_region):
        self.username = username
        self.password = password
        self.pool_id = pool_id
        self.identity_pool_id = identity_pool_id
        self.client_id = client_id
        self.provider = provider
        self.pool_region = pool_region
        self.service = service
        self.service_region = service_region

    def get_auth(self):
        aws = AWSSRP(username=self.username, password=self.password,
                     pool_id=self.pool_id, client_id=self.client_id, pool_region=self.pool_region)

        tokens = aws.authenticate_user()["AuthenticationResult"]
        print(f"Tokens: {tokens}")

        awsidp = AWSIDP(identity_pool_id=self.identity_pool_id, provider=self.provider,
                        id_token=tokens['IdToken'], pool_region=self.pool_region)
        resp = awsidp.get_credentials()

        print(f"Credentials: {resp}")

        access_key_id = resp['access_key']
        secret_access_key = resp['secret_key']
        session_token = resp['session_token']

        return AWS4Auth(access_key_id, secret_access_key, self.service_region, self.service, session_token=session_token)
