'use strict';
const axios = require('axios');
const _ = require('lodash');
const xml2js = require('xml2js');

const {
  AUTH,
  GET_LOC_URL,
  CREATE_LOC_URL,
  SEND_PAYLOAD_URL,
  UPDATE_PAYLOAD_URL,
  TRACKING_NOTES_API_URL,
  API_PASS,
  API_USER_ID,
} = process.env;

async function getLocationId(name, address1, address2, cityName, state, zipCode) {
  const apiUrl = `${GET_LOC_URL}?name=${name}&address1=${address1}&address2=${
    address2 ?? ''
  }&city_name=${cityName}&state=${state}&zip_code=${zipCode}`;

  const headers = {
    Accept: 'application/json',
    Authorization: AUTH,
  };

  try {
    const response = await axios.get(apiUrl, {
      // params: queryParams,
      headers,
    });

    // Handle the response using lodash or other methods as needed
    const responseData = _.get(response, 'data', {});
    console.info('🙂 -> file: apis.js:30 -> getLocationId -> responseData:', responseData);
    // Return the location ID or perform additional processing as needed
    return _.get(responseData, '[0].id', false);
  } catch (error) {
    console.error('🙂 -> file: apis.js:34 -> getLocationId -> error:', error);
    return false;
  }
}

async function createLocation(data) {
  const apiUrl = CREATE_LOC_URL;

  const headers = {
    Accept: 'application/json',
    Authorization: AUTH,
  };

  try {
    const response = await axios.put(apiUrl, data, {
      headers,
    });

    // Handle the response using lodash or other methods as needed
    const responseData = _.get(response, 'data', {});
    console.info('🙂 -> file: apis.js:54 -> createLocation -> responseData:', responseData);
    // Return the created location data or perform additional processing as needed
    return _.get(responseData, 'id', false);
  } catch (error) {
    const errorMessage = _.get(error, 'response.data', error.message);
    console.error('🙂 -> file: apis.js:58 -> createLocation -> error:', error);
    throw new Error(`Error in Create Location API.
    \n Error Details: ${errorMessage}
    \n Payload:
    \n ${JSON.stringify(data)}`);
  }
}

async function sendPayload({ payload: data }) {
  const apiUrl = SEND_PAYLOAD_URL;

  const headers = {
    Accept: 'application/json',
    Authorization: AUTH,
  };

  try {
    const response = await axios.put(apiUrl, data, {
      headers,
    });

    // Handle the response using lodash or other methods as needed
    const responseData = _.get(response, 'data', {});
    console.info('🙂 -> file: apis.js:74 -> responseData:', responseData);
    // Return the created location data or perform additional processing as needed
    return responseData;
  } catch (error) {
    console.info('🙂 -> file: apis.js:78 -> error:', error);
    const errorMessage = _.get(error, 'response.data', error.message);
    throw new Error(`Error in Create Orders API.
    \n Error Details: ${errorMessage}
    \n Payload: 
    \n ${JSON.stringify(data)}`);
  }
}

async function updateOrders({ payload: data }) {
  const apiUrl = UPDATE_PAYLOAD_URL;

  const headers = {
    Accept: 'application/json',
    Authorization: AUTH,
  };

  try {
    const response = await axios.put(apiUrl, data, {
      headers,
    });

    // Handle the response using lodash or other methods as needed
    const responseData = _.get(response, 'data', {});
    console.info('🙂 -> file: apis.js:54 -> updateOrders -> responseData:', responseData);
    // Return the created location data or perform additional processing as needed
    return responseData;
  } catch (error) {
    console.info('🙂 -> file: apis.js:103 -> error:', error);
    const errorMessage = _.get(error, 'response.data', error.message);
    throw new Error(`Error in Update Orders API.
    \n Error Details: ${errorMessage}
    \n Payload:
    \n ${JSON.stringify(data)}`);
  }
}

/**
 * update WorlTrack XML api
 * @param {*} houseBill
 * @param {*} shipmentId
 * @returns
 */

async function liveSendUpdate(houseBill, shipmentId) {
  const requestBody = `<?xml version="1.0" encoding="utf-8"?>
    <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
      <soap:Header>
        <AuthHeader xmlns="http://tempuri.org/">
          <UserName>${API_USER_ID}</UserName>
          <Password>${API_PASS}</Password>
        </AuthHeader>
      </soap:Header>
      <soap:Body>
        <WriteTrackingNote xmlns="http://tempuri.org/">
          <HandlingStation></HandlingStation>
          <HouseBill>${houseBill}</HouseBill>
          <TrackingNotes>
            <TrackingNotes>
              <TrackingNoteMessage>Ivia shipment number ${shipmentId}</TrackingNoteMessage>
            </TrackingNotes>
          </TrackingNotes>
        </WriteTrackingNote>
      </soap:Body>
    </soap:Envelope>`;

  try {
    const response = await axios.post(TRACKING_NOTES_API_URL, requestBody, {
      headers: {
        'Content-Type': 'text/xml',
        'Accept': 'text/xml',
      },
    });

    const xmlParser = new xml2js.Parser({ explicitArray: false });
    const parsedResponse = await xmlParser.parseStringPromise(response.data);

    const result =
      parsedResponse['soap:Envelope']['soap:Body'].WriteTrackingNoteResponse
        .WriteTrackingNoteResult;

    if (result === 'Success') {
      console.info(`updated in tracking notes API with shipmentId: ${shipmentId}`);
      return parsedResponse;
    }
    parsedResponse['soap:Envelope']['soap:Body'].WriteTrackingNoteResponse.WriteTrackingNoteResult =
      'Failed';
    return parsedResponse;
  } catch (error) {
    console.error('Error in liveSendUpdate:', error);
    return error.response ? error.response.data : 'liveSendUpdate error';
  }
}

module.exports = { getLocationId, createLocation, sendPayload, updateOrders, liveSendUpdate };
