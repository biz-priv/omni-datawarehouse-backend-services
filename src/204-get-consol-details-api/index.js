'use strict';

// eslint-disable-next-line import/no-unresolved
const sql = require('mssql');
const _ = require('lodash');

module.exports.handler = async (event) => {
  console.info('Event: ', event);

  try {
    const consolno = _.get(event, 'queryStringParameters.consolno', '');

    // Check if consolno is provided
    if (consolno === '') {
      return {
        statusCode: 400,
        body: JSON.stringify({ error: 'consolno is required in the query parameters' }),
      };
    }
    // Connect to SQL Server
    const request = await connectToSQLServer();
    // Your SQL query here
    const queryResult = await request.query(
      `SELECT fk_orderno FROM  dbo.tbl_consoldetail WHERE consolno = '${consolno}'`
    );
    console.info('🚀 ~ file: index.js:24 ~ queryResult:', queryResult);

    const transformedResult = _.get(queryResult, 'recordset', []).map((item) => item.fk_orderno);

    // Return the transformed result as a JSON response
    const response = {
      statusCode: 200,
      body: JSON.stringify(transformedResult),
    };
    console.info('🚀 ~ file: index.js:34 ~ response:', response);

    return response;
  } catch (error) {
    console.error('Error executing SQL query: ', error);
    const response = {
      statusCode: 500,
      body: JSON.stringify({ error: 'Internal Server Error' }),
    };

    return response;
  } finally {
    await sql.close();
    console.info('Closed SQL connection');
  }
};

// Function to connect to SQL Server
async function connectToSQLServer() {
  const config = {
    user: process.env.DB_USERNAME,
    password: process.env.DB_PASSWORD,
    server: process.env.DB_SERVER,
    port: parseInt(process.env.DB_PORT, 10),
    database: process.env.DB_DATABASE,
    options: {
      trustServerCertificate: true,
    },
  };
  console.info('🚀 ~ file: index.js:62 ~ config:', config);
  try {
    await sql.connect(config);
    const request = new sql.Request();
    console.info('Connected to SQL Server');
    return request;
  } catch (err) {
    console.error('Error: ', err);
    throw err;
  }
}
