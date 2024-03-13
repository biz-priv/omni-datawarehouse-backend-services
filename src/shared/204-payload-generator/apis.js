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
  ADDRESS_MAPPING_G_API_KEY,
} = process.env;

const apiKey = ADDRESS_MAPPING_G_API_KEY;

async function getLocationId(name, address1, address2, state) {
  const apiUrl = `${GET_LOC_URL}?name=${name}&address1=${address1}&address2=${
    address2 ?? ''
  }&state=${state}`;

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

async function createLocation({ data, orderId, consolNo = 0, country, houseBill }) {
  if (country.toLowerCase() === 'us' && _.get(data, 'zip_code', 0).length > 5) {
    data.zip_code = data.zip_code.slice(0, 5);
  }
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
    throw new Error(`\nError in Create Location API.
    \n FK_OrderNo: ${orderId}
    \n ConsolNo: ${consolNo}
    \n Housebill: ${houseBill}
    \n Error Details: ${errorMessage}
    \n Payload:
    \n ${JSON.stringify(data)}`);
  }
}

async function sendPayload({ payload: data, orderId, consolNo = 0, houseBillString }) {
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
    throw new Error(`\nError in Create Orders API.
    \n FK_OrderNo: ${orderId}
    \n ConsolNo: ${consolNo}
    \n Housebill: ${houseBillString}
    \n Error Details: ${errorMessage}
    \n Payload: 
    \n ${JSON.stringify(data)}`);
  }
}

async function updateOrders({ payload: data, orderId, consolNo = 0, houseBillString }) {
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
    throw new Error(`\nError in Update Orders API.
    \n FK_OrderNo: ${orderId}
    \n ConsolNo: ${consolNo}
    \n Housebill: ${houseBillString}
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
              <TrackingNoteMessage>LiVe Logistics shipment number ${shipmentId}</TrackingNoteMessage>
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

async function checkAddressByGoogleApi(address) {
  try {
    const geocodeResponse = await axios.get(
      `https://maps.googleapis.com/maps/api/geocode/json?address=${encodeURIComponent(address)}&key=${apiKey}`
    );

    if (geocodeResponse.data.status !== 'OK') {
      throw new Error(`Unable to geocode ${address}`);
    }

    const { lat, lng } = _.get(geocodeResponse, 'data.results[0].geometry.location', {});
    return { lat, lng };
  } catch (error) {
    console.error(`Error geocoding address "${address}":`, error.message);
    throw error;
  }
}

async function getTimezoneByGoogleApi(lat, long) {
  try {
    const timestamp = Date.now() / 1000;
    const timezoneResponse = await axios.get(
      `https://maps.googleapis.com/maps/api/timezone/json?location=${lat}%2C${long}&timestamp=${timestamp}&key=${apiKey}`
    );

    if (timezoneResponse.data.status !== 'OK') {
      throw new Error(`Unable to fetch timezone for coordinates (${lat}, ${long})`);
    }

    const timeZoneId = _.get(timezoneResponse, 'data.timeZoneId');
    return timeZoneId;
  } catch (error) {
    console.error(`Error fetching timezone for coordinates (${lat}, ${long}):`, error.message);
    throw error;
  }
}

module.exports = {
  getLocationId,
  createLocation,
  sendPayload,
  updateOrders,
  liveSendUpdate,
  checkAddressByGoogleApi,
  getTimezoneByGoogleApi,
};
