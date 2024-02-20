'use strict';

const axios = require('axios');
const AWS = require('aws-sdk');
const { get } = require('lodash');
const uuid = require('uuid');
const xml2js = require('xml2js');

const sns = new AWS.SNS();
const {
  prepareHeaderLevelAndReferenceListData,
  prepareShipmentListData,
  getS3Data,
  putItem,
} = require('./helper');

let dynamoData = {};
let s3folderName = '';

module.exports.handler = async (event, context) => {
  console.info('event: ', event);
  try {
    const s3Bucket = get(event, 'Records[0].s3.bucket.name', '');
    const s3Key = get(event, 'Records[0].s3.object.key', '');
    dynamoData = { s3Bucket, id: uuid.v4().replace(/[^a-zA-Z0-9]/g, '') };
    s3folderName = s3Key.split('/')[1];
    console.info('Id :', get(dynamoData, 'id', ''));

    const s3Data = await getS3Data(s3Bucket, s3Key);

    const xmlObj = await xmlToJson(s3Data);
    dynamoData.xmlObj = xmlObj;

    const xmlWTPayload = await prepareWTpayload(get(xmlObj, 'UniversalInterchange.Body', ''));
    dynamoData.xmlWTPayload = xmlWTPayload;

    const xmlWTResponse = await sendToWT(xmlWTPayload);
    dynamoData.xmlWTResponse = xmlWTResponse;

    const xmlWTObjResponse = await xmlToJson(xmlWTResponse);
    dynamoData.xmlWTObjResponse = xmlWTObjResponse;

    const xmlCWPayload = await prepareCWpayload(xmlObj, xmlWTObjResponse);
    dynamoData.xmlCWPayload = xmlCWPayload;

    const xmlCWResponse = await sendToCW(xmlCWPayload);
    dynamoData.xmlCWResponse = xmlCWResponse;
    dynamoData.status = 'SUCCESS';

    await putItem(dynamoData);

    console.info('xml object: ', xmlObj);
    return 'Success';
  } catch (error) {
    console.error('Main lambda error: ', error);

    try {
      const params = {
        Message: `An error occurred in function ${context.functionName}. Error details: ${error}.\n id: ${get(dynamoData, 'id', '')} \n Note: Use the id for better search in the logs.`,
        TopicArn: process.env.ERROR_SNS_ARN,
      };
      await sns.publish(params).promise();
    } catch (err) {
      console.error('Error while sending sns notification: ', err);
    }
    dynamoData.errorMsg = error;
    dynamoData.status = 'FAILED';
    await putItem(dynamoData);
    return false;
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
    dynamoData.xmlWTResponse = get(res, 'data', '');
    throw new Error(`API Request Failed: ${res}`);
  } catch (error) {
    console.error('e:addMilestoneApi', error);
    throw error;
  }
}

async function sendToCW(postData) {
  try {
    const config = {
      url: process.env.CW_URL,
      method: 'post',
      headers: {
        Authorization: 'Basic dHJ4dHMyOjY0ODg1Nw==',
      },
      data: postData,
    };

    console.info('config: ', config);
    const res = await axios.request(config);
    if (get(res, 'status', '') === 200) {
      return get(res, 'data', '');
    }
    dynamoData.xmlWTResponse = get(res, 'data', '');
    throw new Error(`API Request Failed: ${res}`);
  } catch (error) {
    console.error('e:addMilestoneApi', error);
    throw error;
  }
}

async function prepareWTpayload(xmlObj) {
  try {
    const headerAndReferenceListData = await prepareHeaderLevelAndReferenceListData(
      xmlObj,
      s3folderName
    );
    const shipmentListData = await prepareShipmentListData(xmlObj, s3folderName);
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
  dynamoData.fileNumber = get(
    xmlWTObjResponse,
    'soap:Envelope.soap:Body.AddNewShipmentV3Response.AddNewShipmentV3Result.ShipQuoteNo',
    ''
  );
  try {
    const payload = {
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
    };

    return payload;
  } catch (error) {
    console.error('Error in prepareCWpayload: ', error);
    throw error;
  }
}
