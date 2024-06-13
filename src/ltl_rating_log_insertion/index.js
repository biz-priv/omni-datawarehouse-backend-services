/*
* File: src\ltl_rating_log_insertion\index.js
* Project: Omni-datawarehouse-backend-services
* Author: Bizcloud Experts
* Date: 2023-12-18
* Confidential and Proprietary
*/
const { get } = require("lodash");
const { v4 } = require("uuid");
const { LTL_LOG_TABLE } = process.env;
const AWS = require("aws-sdk");
const dynamoDB = new AWS.DynamoDB.DocumentClient();
const moment = require("moment-timezone");
let functionName;
module.exports.handler = async (event, context) => {
    console.info(`🙂 -> file: index.js:2 -> event:`, event);
    functionName = get(context, "functionName");
    const records = get(event, "Records", []);
    await Promise.all(records.map(async (record) => await processRecord(JSON.parse(get(record, "body", "")))));
};

async function processRecord(record) {
    const reference = get(record, "payloadForQueue.[0].reference", v4());
    await Promise.all(
        get(record, "payloadForQueue", []).map(async (data, index) => {
            console.info(`🙂 -> file: index.js:16 -> index:`, index);
            console.info(`🙂 -> file: index.js:17 -> data:`, data);
            const pKey = reference;
            const sKey = index === 0 ? "Main" : get(data, "carrier", index);
            const status = get(data, "status");
            const payload = get(data, "payload");
            const response = get(data, "response");
            await insertIntoTransactionTable({ pKey, sKey, status, payload, response });
        })
    );
}

async function insertIntoTransactionTable({ pKey, sKey, status, payload, response }) {
    const insertParams = {
        TableName: LTL_LOG_TABLE,
        Item: {
            pKey,
            sKey,
            status,
            payload,
            response,
            expiration: Math.floor(new Date(moment().add(7, "days").format()).getTime() / 1000),
            lastUpdateId: functionName,
            lastUpdateTime: moment.tz("America/Chicago").format(),
        },
    };
    console.info(`🙂 -> file: index.js:29 -> insertParams:`, insertParams);
    try {
        return dynamoDB.put(insertParams).promise();
    } catch (err) {
        console.error(`🙂 -> file: index.js:32 -> err:`, err);
        throw err;
    }
}
