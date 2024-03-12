'use strict';

const axios = require('axios');
const AWS = require('aws-sdk');
const { get } = require('lodash');
const uuid = require('uuid');
const xml2js = require('xml2js');
const moment = require('moment-timezone');

const sns = new AWS.SNS();
const {
  prepareHeaderLevelAndReferenceListData,
  prepareShipmentListData,
  getS3Data,
  putItem,
} = require('./helper');

let dynamoData = {};
let s3Bucket = '';
let s3Key = '';

module.exports.handler = async (event, context) => {
  console.info(JSON.stringify(event));
  try {
    s3Bucket = get(event, 'Records[0].s3.bucket.name', '');
    s3Key = get(event, 'Records[0].s3.object.key', '');
    dynamoData = { S3Bucket: s3Bucket, S3Key: s3Key, Id: uuid.v4().replace(/[^a-zA-Z0-9]/g, '') };
    console.info('Id :', get(dynamoData, 'Id', ''));

    // Set the time zone to CST
    const cstDate = moment().tz('America/Chicago');
    dynamoData.CSTDate = cstDate.format('YYYY-MM-DD');
    dynamoData.CSTDateTime = cstDate.format('YYYY-MM-DD HH:mm:ss');

    // Extract data from s3.
    const s3Data = await getS3Data(s3Bucket, s3Key);
    dynamoData.XmlFromCW = s3Data;

    // Convert the XML data from s3 to json.
    const xmlObjFromCW = await xmlToJson(s3Data);

    const xmlObj = get(xmlObjFromCW, 'UniversalInterchange.Body', '');

    const statusCode = get(xmlObj, 'UniversalShipment.Shipment.Order.Status.Code', '');
    dynamoData.StatusCode = statusCode;

    // Preparing payload to send the data to world trak.
    const xmlWTPayload = await prepareWTpayload(xmlObj, statusCode);
    dynamoData.XmlWTPayload = xmlWTPayload;

    // Sending the payload to world trak endpoint.
    const xmlWTResponse = await sendToWT(xmlWTPayload);
    dynamoData.XmlWTResponse = xmlWTResponse;
    dynamoData.Status = 'payload_sent_to_wt';

    // Convert the response which we receive from world trak to json.
    const xmlWTObjResponse = await xmlToJson(xmlWTResponse);

    // Verifying if there is any error in the World trak api call.
    if (
      get(
        xmlWTObjResponse,
        'soap:Envelope.soap:Body.AddNewShipmentV3Response.AddNewShipmentV3Result.ErrorMessage',
        ''
      ) !== '' ||
      get(
        xmlWTObjResponse,
        'soap:Envelope.soap:Body.AddNewShipmentV3Response.AddNewShipmentV3Result.Housebill',
        ''
      ) === ''
    ) {
      throw new Error(
        `WORLD TRAK API call failed: ${get(
          xmlWTObjResponse,
          'soap:Envelope.soap:Body.AddNewShipmentV3Response.AddNewShipmentV3Result.ErrorMessage',
          ''
        )}`
      );
    }

    // Preparing payload to send the data to cargowise
    const xmlCWPayload = await prepareCWpayload(xmlObj, xmlWTObjResponse);
    dynamoData.XmlCWPayload = xmlCWPayload;

    // Sending the payload to cargowise endpoint
    const xmlCWResponse = await sendToCW(xmlCWPayload);
    dynamoData.XmlCWResponse = xmlCWResponse;

    // Convert the response which we receive from cargowise to json
    const xmlCWObjResponse = await xmlToJson(xmlCWResponse);

    console.info(xmlCWObjResponse);
    console.info(xmlCWObjResponse.UniversalResponse.Data.UniversalEvent.Event.EventType);

    const EventType = get(
      xmlCWObjResponse,
      'UniversalResponse.Data.UniversalEvent.Event.EventType',
      ''
    );
    const contentType = get(
      get(
        xmlCWObjResponse,
        'UniversalResponse.Data.UniversalEvent.Event.ContextCollection.Context',
        []
      ).filter((obj) => obj.Type === 'ProcessingStatusCode'),
      '[0].Value',
      ''
    );

    console.info('EventType: ', EventType, 'contentType', contentType);

    if (EventType !== 'DIM' && contentType !== 'PRS') {
      throw new Error(
        `CARGOWISE API call failed: ${get(xmlCWObjResponse, 'UniversalResponse.ProcessingLog', '')}`
      );
    }

    dynamoData.Status = 'SUCCESS';

    // Storing the complete logs information into a dynamodb table
    await putItem(dynamoData);
    return 'Success';
  } catch (error) {
    console.error('Main lambda error: ', error);

    try {
      const params = {
        Message: `An error occurred in function ${context.functionName}.\n\nERROR DETAILS: ${error}.\n\nId: ${get(dynamoData, 'Id', '')}.\n\nEVENT: ${JSON.stringify(event)}.\n\nS3BUCKET: ${s3Bucket}.\n\nS3KEY: ${s3Key}.\n\nLAMBDA TRIGGER: This lambda will trigger when there is a XML file dropped in a s3 Bucket(for s3 bucket and the file path, please refer to the event).\n\nRETRIGGER PROCESS: After fixing the issue, please retrigger the process by reuploading the file mentioned in the event.\n\nNote: Use the id: ${get(dynamoData, 'Id', '')} for better search in the logs and also check in dynamodb: ${process.env.LOGS_TABLE} for understanding the complete data.`,
        Subject: `CW to WT Create Shipment ERROR ${context.functionName}`,
        TopicArn: process.env.SNS_TOPIC_ARN,
      };
      await sns.publish(params).promise();
      console.info('SNS notification has sent');
    } catch (err) {
      console.error('Error while sending sns notification: ', err);
    }
    dynamoData.ErrorMsg = `${error}`;
    dynamoData.Status = 'FAILED';
    await putItem(dynamoData);
    return 'Failed';
  }
};

async function sendToWT(postData) {
  try {
    const config = {
      url: process.env.WT_URL,
      method: 'post',
      headers: {
        'Content-Type': 'text/xml',
        'soapAction': 'http://tempuri.org/AddNewShipmentV3',
      },
      data: postData,
    };

    console.info('config: ', config);
    const res = await axios.request(config);
    if (get(res, 'status', '') === 200) {
      return get(res, 'data', '');
    }
    throw new Error(`WORLD TRAK API Request Failed: ${res}`);
  } catch (error) {
    console.error('WORLD TRAK API Request Failed: ', error);
    throw error;
  }
}

async function sendToCW(postData) {
  try {
    const config = {
      url: process.env.CW_URL,
      method: 'post',
      headers: {
        Authorization: process.env.CW_ENDPOINT_AUTHORIZATION,
      },
      data: postData,
    };

    console.info('config: ', config);
    const res = await axios.request(config);
    if (get(res, 'status', '') === 200) {
      return get(res, 'data', '');
    }
    throw new Error(`CARGOWISE API Request Failed: ${res}`);
  } catch (error) {
    console.error('CARGOWISE API Request Failed: ', error);
    throw error;
  }
}

async function prepareWTpayload(xmlObj, statusCode) {
  try {
    const headerAndReferenceListData = await prepareHeaderLevelAndReferenceListData(
      xmlObj,
      statusCode
    );
    const shipmentListData = await prepareShipmentListData(xmlObj);
    console.info(headerAndReferenceListData);
    console.info(shipmentListData);

    const finalData = {
      oshipData: {
        ...headerAndReferenceListData,
        ...shipmentListData,
      },
    };

    const xmlBuilder = new xml2js.Builder({
      render: {
        pretty: true,
        indent: '    ',
        newline: '\n',
      },
    });

    const xmlPayload = xmlBuilder.buildObject({
      'soap:Envelope': {
        '$': {
          'xmlns:xsi': 'http://www.w3.org/2001/XMLSchema-instance',
          'xmlns:xsd': 'http://www.w3.org/2001/XMLSchema',
          'xmlns:soap': 'http://schemas.xmlsoap.org/soap/envelope/',
        },
        'soap:Header': {
          AuthHeader: {
            $: {
              xmlns: 'http://tempuri.org/',
            },
            UserName: process.env.wt_soap_username,
            Password: process.env.wt_soap_password,
          },
        },
        'soap:Body': {
          AddNewShipmentV3: {
            $: {
              xmlns: 'http://tempuri.org/',
            },
            oShipData: finalData.oshipData, // Insert your JSON data here
          },
        },
      },
    });

    return xmlPayload;
  } catch (error) {
    console.error('Error While preparing payload: ', error);
    throw error;
  }
}

async function xmlToJson(xmlData) {
  try {
    const parser = new xml2js.Parser({
      explicitArray: false,
      mergeAttrs: true,
    });
    return await parser.parseStringPromise(xmlData);
  } catch (error) {
    console.error('Error in xmlToJson: ', error);
    throw error;
  }
}

async function prepareCWpayload(xmlObj, xmlWTObjResponse) {
  dynamoData.Housebill = get(
    xmlWTObjResponse,
    'soap:Envelope.soap:Body.AddNewShipmentV3Response.AddNewShipmentV3Result.Housebill',
    ''
  );
  dynamoData.FileNumber = get(
    xmlWTObjResponse,
    'soap:Envelope.soap:Body.AddNewShipmentV3Response.AddNewShipmentV3Result.ShipQuoteNo',
    ''
  );
  try {
    const builder = new xml2js.Builder({
      headless: true,
    });

    const payload = builder.buildObject({
      UniversalShipment: {
        $: {
          'xmlns:ns0': 'http://www.cargowise.com/Schemas/Universal/2011/11',
        },
        Shipment: {
          $: {
            xmlns: 'http://www.cargowise.com/Schemas/Universal/2011/11',
          },
          DataContext: {
            DataTargetCollection: {
              DataTarget: {
                Type: 'WarehouseOrder',
                Key: get(
                  xmlObj,
                  'UniversalShipment.Shipment.DataContext.DataSourceCollection.DataSource.Key',
                  ''
                ),
              },
            },
          },
          Order: {
            OrderNumber: get(xmlObj, 'UniversalShipment.Shipment.Order.OrderNumber', ''),
            TransportReference: get(
              xmlWTObjResponse,
              'soap:Envelope.soap:Body.AddNewShipmentV3Response.AddNewShipmentV3Result.Housebill',
              ''
            ),
          },
        },
      },
    });

    return payload;
  } catch (error) {
    console.error('Error in prepareCWpayload: ', error);
    throw error;
  }
}
