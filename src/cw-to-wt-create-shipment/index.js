'use strict';

const axios = require('axios');
const AWS = require('aws-sdk');
const { get } = require('lodash');
const uuid = require('uuid');
const xml2js = require('xml2js');
const moment = require('moment-timezone');

const { Converter } = AWS.DynamoDB;
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
let shipmentId = '';
let eventType = '';

// Set the time zone to CST
const cstDate = moment().tz('America/Chicago');

module.exports.handler = async (event, context) => {
  console.info(JSON.stringify(event));
  let referenceNo;
  try {
    if (get(event, 'Records[0].eventSource', '') === 'aws:dynamodb') {
      dynamoData = Converter.unmarshall(get(event, 'Records[0].dynamodb.NewImage'), '');
      s3Bucket = get(dynamoData, 'S3Bucket', '');
      s3Key = get(dynamoData, 'S3Key', '');
      shipmentId = get(dynamoData, 'ShipmentId', '');
      eventType = 'dynamo';
      console.info('Id :', get(dynamoData, 'Id', ''));
    } else {
      eventType = 's3';
      s3Bucket = get(event, 'Records[0].s3.bucket.name', '');
      s3Key = get(event, 'Records[0].s3.object.key', '');
      shipmentId = get(get(s3Key.split('/'), '[2]', '').split('_'), '[3]', '');
      dynamoData = {
        S3Bucket: s3Bucket,
        S3Key: s3Key,
        Id: uuid.v4().replace(/[^a-zA-Z0-9]/g, ''),
        ShipmentId: shipmentId,
      };
      console.info('Id :', get(dynamoData, 'Id', ''));
      dynamoData.CSTDate = cstDate.format('YYYY-MM-DD');
      dynamoData.CSTDateTime = cstDate.format('YYYY-MM-DD HH:mm:ss');
      // Extract data from s3.
      const s3Data = await getS3Data(s3Bucket, s3Key);
      dynamoData.XmlFromCW = s3Data;
    }

    // Convert the XML data from s3 to json.
    const xmlObjFromCW = await xmlToJson(get(dynamoData, 'XmlFromCW', ''));

    const xmlObj = get(xmlObjFromCW, 'UniversalInterchange.Body', {});

    const statusCode = get(xmlObj, 'UniversalShipment.Shipment.Order.Status.Code', '');
    dynamoData.StatusCode = statusCode;

    referenceNo = get(xmlObj, 'UniversalShipment.Shipment.Order.OrderNumber', '');
    dynamoData.ReferenceNo = referenceNo;

    // Preparing payload to send the data to world trak.
    const xmlWTPayload = await prepareWTpayload(xmlObj, statusCode);
    dynamoData.XmlWTPayload = xmlWTPayload;

    if (eventType === 'dynamo') {
      const refNo = get(xmlObj, 'UniversalShipment.Shipment.Order.OrderNumber', '');
      const checkHousebill = await checkHousebillExists(refNo);
      if (checkHousebill !== '') {
        console.info(
          `Housebill already created : The housebill number for '${refNo}' already created in WT and the Housebill No. is '${checkHousebill}'`
        );
        await cwProcess(xmlObj, checkHousebill);
        try {
          const params = {
            Message: `Shipment is created successfully after retry.\n\nShipmentId: ${get(dynamoData, 'ShipmentId', '')}.\n\nId: ${get(dynamoData, 'Id', '')}.\n\nS3BUCKET: ${s3Bucket}.\n\nS3KEY: ${s3Key}`,
            Subject: `${process.env.STAGE} - CW to WT Create Shipment Reprocess Success for ShipmentId: ${get(dynamoData, 'ShipmentId', '')}`,
            TopicArn: process.env.NOTIFICATION_ARN,
          };
          await sns.publish(params).promise();
          console.info('SNS notification has sent');
        } catch (err) {
          console.error('Error while sending sns notification: ', err);
        }
        await putItem(dynamoData);
        return `Housebill already created : The housebill number for '${refNo}' already created in WT and the Housebill No. is '${checkHousebill}'`;
      }
    }

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
    await cwProcess(xmlObj, dynamoData.Housebill);

    if (eventType === 'dynamo') {
      try {
        const params = {
          Message: `Shipment is created successfully after retry.\n\nShipmentId: ${get(dynamoData, 'ShipmentId', '')}.\n\nId: ${get(dynamoData, 'Id', '')}.\n\nS3BUCKET: ${s3Bucket}.\n\nS3KEY: ${s3Key}`,
          Subject: `${process.env.STAGE} - CW to WT Create Shipment Reprocess Success for ShipmentId: ${get(dynamoData, 'ShipmentId', '')}`,
          TopicArn: process.env.NOTIFICATION_ARN,
        };
        await sns.publish(params).promise();
        console.info('SNS notification has sent');
      } catch (err) {
        console.error('Error while sending sns notification: ', err);
      }
    }

    // Storing the complete logs information into a dynamodb table
    await putItem(dynamoData);
    return 'Success';
  } catch (error) {
    console.error('Main lambda error: ', error);

    try {
      const params = {
        Message: `An error occurred in function ${context.functionName}.\n\nERROR DETAILS: ${error}.\n\nId: ${get(dynamoData, 'Id', '')}.\n\nEVENT: ${JSON.stringify(event)}.\n\nS3BUCKET: ${s3Bucket}.\n\nS3KEY: ${s3Key}.\n\nLAMBDA TRIGGER: This lambda will trigger when there is a XML file dropped in a s3 Bucket(for s3 bucket and the file path, please refer to the event).\n\nRETRIGGER PROCESS: After fixing the issue, please retrigger the process by reuploading the file mentioned in the event.\n\nNote: Use the id: ${get(dynamoData, 'Id', '')} for better search in the logs and also check in dynamodb: ${process.env.LOGS_TABLE} for understanding the complete data.`,
        Subject: `CW to WT Create Shipment ERROR ${context.functionName}`,
        TopicArn: process.env.NOTIFICATION_ARN,
      };
      await sns.publish(params).promise();
      console.info('SNS notification has sent');
    } catch (err) {
      console.error('Error while sending sns notification: ', err);
    }
    dynamoData.ErrorMsg = `${error}`;
    dynamoData.Status = 'FAILED';
    if (eventType === 'dynamo') {
      dynamoData.RetryCount = String(Number(dynamoData.RetryCount) + 1);
    } else {
      dynamoData.RetryCount = '0';
    }
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
            oShipData: finalData.oshipData,
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

async function prepareCWpayload(xmlObj, housebill) {
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
            TransportReference: housebill,
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

async function checkHousebillExists(referenceNo) {
  try {
    const builder = new xml2js.Builder({
      headless: true,
    });

    const payload = builder.buildObject({
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
            UserName: process.env.CHECK_HOUSEBILL_EXISTS_API_USERNAME,
            Password: process.env.CHECK_HOUSEBILL_EXISTS_API_PASSWORD,
          },
        },
        'soap:Body': {
          GetShipmentsByReferenceNo: {
            $: {
              xmlns: 'http://tempuri.org/',
            },
            RefNo: referenceNo,
            RefType: 'S',
          },
        },
      },
    });

    const xmlResponse = await sendToCheckHousebillExists(payload);
    const jsonResponse = await xmlToJson(xmlResponse);

    const shipmentDetails = get(
      jsonResponse,
      'soap:Envelope.soap:Body.GetShipmentsByReferenceNoResponse.GetShipmentsByReferenceNoResult.ShipmentDetail',
      {}
    );
    let housebill = '';
    if (Array.isArray(shipmentDetails)) {
      const housebills = shipmentDetails.map((detail) => {
        const trackingNo = get(detail, 'TrackingNo', '');
        return trackingNo.length > 4 ? trackingNo.substring(4) : '';
      });

      const numericHousebills = housebills
        .map((housebillNo) => parseInt(housebillNo, 10))
        .filter(Number.isFinite);

      housebill = numericHousebills
        .reduce((max, current) => {
          return current > max ? current : max;
        }, 0)
        .toString();
    } else if (Object.keys(shipmentDetails).length > 0) {
      const trackingNo = get(shipmentDetails, 'TrackingNo', '');
      housebill = trackingNo.length > 4 ? trackingNo.substring(4) : '';
    } else {
      housebill = '';
    }

    return housebill;
  } catch (error) {
    console.error('Error in prepareCWpayload: ', error);
    throw error;
  }
}

async function sendToCheckHousebillExists(postData) {
  try {
    const config = {
      url: process.env.CHECK_HOUSEBILL_EXISTS_API_URL,
      method: 'post',
      headers: {
        'Content-Type': 'text/xml',
      },
      data: postData,
    };

    console.info('config: ', config);
    const res = await axios.request(config);
    if (get(res, 'status', '') === 200) {
      return get(res, 'data', '');
    }
    throw new Error(`Check for Housebill API Request Failed: ${res}`);
  } catch (error) {
    console.error('Check for Housebill API Request Failed: ', error);
    throw error;
  }
}

async function cwProcess(xmlObj, housebill) {
  try {
    // Preparing payload to send the data to cargowise
    const xmlCWPayload = await prepareCWpayload(xmlObj, housebill);
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
  } catch (error) {
    console.error('Error in cwProcess:', error);
    throw error;
  }
}
