const AWS = require("aws-sdk");
const { get } = require("lodash");
const { v4 } = require("uuid");
const s3 = new AWS.S3({ region: process.env.REGION });
const ses = new AWS.SES({ region: process.env.REGION });
const sqs = new AWS.SQS({ region: process.env.REGION });
const XLSX = require("xlsx");
const moment = require("moment-timezone");
const { putObject, checkIfObjectExists } = require("../shared/s3");
let limit = 10;
let offset = null;
module.exports.handler = async (event, context, callback) => {
    try {
        console.info(`🙂 -> file: index.js:4 -> event:`, JSON.stringify(event));
        if (get(event, "Records")) {
            const records = get(event, "Records", []);
            console.info(`🙂 -> file: index.js:12 -> records:`, records);
            return await Promise.all(
                records.map(async (record) => {
                    const body = JSON.parse(get(record, "body"));
                    console.info(`🙂 -> file: index.js:12 -> body:`, body);
                    const s3Body = get(body, "Records[0].s3");
                    console.info(`🙂 -> file: index.js:15 -> s3Body:`, s3Body);
                    const bucket = get(s3Body, "bucket.name");
                    console.info(`🙂 -> file: index.js:17 -> bucket:`, bucket);
                    const key = get(s3Body, "object.key");
                    console.info(`🙂 -> file: index.js:19 -> key:`, key);
                    await startNextStep({ offset: 0, bucket, key });
                    const receiptHandle = get(record, "receiptHandle", null);
                    return await deleteSqsMessage(receiptHandle);
                })
            );
        }
        offset = parseInt(get(event, "offset", 0));

        const bucket = get(event, "bucket");
        console.info(`🙂 -> file: index.js:30 -> bucket:`, bucket);

        const key = get(event, "key", "");
        console.info(`🙂 -> file: index.js:32 -> key:`, key);

        const s3Object = await getExcel(bucket, key);
        console.info(`🙂 -> file: index.js:31 -> s3Object:`, s3Object);

        if (!s3Object) throw new Error("Error getting file form s3.");

        const totalLength = s3Object.length;
        console.info(`🙂 -> file: index.js:42 -> totalLength:`, totalLength);

        const payload = sliceArrayIntoChunks(s3Object, limit, offset);

        let allPayload = await doApiReq(payload);
        allPayload = flattenArray(allPayload);

        const fileName = key.split("/")[1];
        const existingFileKey = `output/${fileName}`;

        let existingData = [];
        console.info(`🙂 -> file: index.js:48 -> existingFileKey:`, existingFileKey);

        const objectExists = await checkIfObjectExists(bucket, existingFileKey);
        console.info(`🙂 -> file: index.js:49 -> objectExists:`, objectExists);

        if (objectExists) existingData = await getExcel(bucket, existingFileKey);
        console.info(`🙂 -> file: index.js:43 -> existingData:`, existingData);

        const finalData = existingData.concat(allPayload);
        console.info(`🙂 -> file: index.js:47 -> finalData:`, finalData);

        const newSheet = XLSX.utils.json_to_sheet(finalData);
        console.info(`🙂 -> file: excelTest.js:36 -> allPayload:`, newSheet);

        const workbook = XLSX.utils.book_new();
        XLSX.utils.book_append_sheet(workbook, newSheet, "Result");
        const s3Body = XLSX.write(workbook, { type: "buffer", bookType: "xlsx" });
        await uploadToS3(s3Body, existingFileKey);
        const updatedOffset = offset + limit;
        console.info(`🙂 -> file: index.js:63 -> updatedOffset:`, updatedOffset);

        if (updatedOffset < totalLength) {
            return callback(null, { hasMoreData: "true", offset: updatedOffset, bucket, key });
        } else {
            const fromEmail = "omnidev@bizcloudexperts.com";
            const toEmail = ["jahir.uddin@bizcloudexperts.com", "krithika.shivaram@bizcloudexperts.com"];
            const subject = "Rating result.";
            const getPresignedUrl = await getS3PresignedUrl(bucket, existingFileKey);
            console.info(`🙂 -> file: index.js:85 -> getPresignedUrl:`, getPresignedUrl);
            if (!getPresignedUrl) throw new Error("Failed to generate Presigned Url");
            const html = `<h3>Rating result.</h3><br/><h4>Here is the link to the file: </h4> <a href="${getPresignedUrl}">${getPresignedUrl}</a>
            <br/><strong style="color: red">NOTE: This link is valid for 10 minutes.</strong>`;
            await sendMailWithoutAttachment({ fromEmail, toEmail, html, subject });
            return callback(null, { hasMoreData: "false" });
        }
    } catch (err) {
        console.info(`🙂 -> file: index.js:39 -> err:`, err);
        return callback(null, { hasMoreData: "false" });
    }
};

async function startNextStep(input) {
    try {
        const params = {
            stateMachineArn: process.env.LTL_BATCH_RATING_STATE_MACHINE,
            input: JSON.stringify(input),
        };
        const stepfunctions = new AWS.StepFunctions();
        await stepfunctions.startExecution(params).promise();
        return true;
    } catch (error) {
        console.error("Error in startNextStep:", error);
        throw error;
    }
}

async function getExcel(bucket, key) {
    return new Promise(async (resolve, reject) => {
        try {
            let item = [];
            const streamGzipFile = s3
                .getObject({
                    Bucket: bucket,
                    Key: key,
                })
                .createReadStream();

            streamGzipFile
                .on("data", (data) => {
                    if (data == "") {
                        console.info(`No data from file: ${data}`);
                    } else {
                        item.push(data);
                    }
                })
                .on("error", function (data) {
                    console.error(`Got an error: ${data}`);
                    if (data === "NoSuchKey: The specified key does not exist.") resolve([]);
                    reject(false);
                })
                .on("end", async () => {
                    const buff = Buffer.concat(item);
                    const wb = XLSX.read(buff);
                    const first_ws = wb.Sheets[wb.SheetNames[0]];
                    const data = XLSX.utils.sheet_to_json(first_ws);
                    resolve(data);
                });
        } catch (error) {
            console.error("Error while reading data line by line", error);
            reject(false);
        }
    });
}

function flattenArray(arrayData) {
    return arrayData.reduce((acc, val) => acc.concat(val), []);
}

async function doApiReq(toJSON) {
    return Promise.all(
        toJSON.map(async (row) => {
            const reference = v4();
            const payload = {
                ltlRateRequest: {
                    pickupTime: moment().tz("America/Chicago").format(),
                    insuredValue: 0,
                    reference,
                    shipperZip: get(row, "ShipperZip"),
                    consigneeZip: get(row, "ConsigneeZip"),
                    shipmentLines: [
                        {
                            pieces: get(row, "Pieces"),
                            pieceType: "PCE",
                            weight: get(row, "WeightLBs"),
                            weightUOM: "lb",
                            length: get(row, "Length1"),
                            width: get(row, "Width1"),
                            height: get(row, "Height1"),
                            dimUOM: "in",
                            hazmat: get(row, "Hazardous"),
                            freightClass: get(row, "Class"),
                        },
                    ],
                    accessorialList: [],
                },
            };
            if (get(row, "PickInside")) payload["ltlRateRequest"]["accessorialList"].push("INSPU");
            if (get(row, "PickResi")) payload["ltlRateRequest"]["accessorialList"].push("RESID");
            if (get(row, "PickAppt")) payload["ltlRateRequest"]["accessorialList"].push("APPT");
            if (get(row, "DropAppt")) payload["ltlRateRequest"]["accessorialList"].push("APPTD");
            if (get(row, "DropInside")) payload["ltlRateRequest"]["accessorialList"].push("INDEL");
            if (get(row, "DropResi")) payload["ltlRateRequest"]["accessorialList"].push("RESDE");
            console.info(`🙂 -> file: index.js:151 -> payload:`, JSON.stringify(payload));
            const response = await callLtlCarrierApi(payload);
            const ratesArray = get(response, "ltlRateResponse", []);
            const data = ratesArray.map((rate) => ({ reference, shipperZip: get(row, "ShipperZip"), consigneeZip: get(row, "ConsigneeZip"), pieces: get(row, "Pieces"), weight: get(row, "WeightLBs"), length: get(row, "Length1"), width: get(row, "Width1"), height: get(row, "Height1"), hazmat: get(row, "Hazardous"), freightClass: get(row, "Class"), carrier: get(rate, "carrier"), transitDays: get(rate, "transitDays"), quoteNumber: get(rate, "quoteNumber", ""), totalRate: get(rate, "totalRate"), serviceLevel: get(rate, "serviceLevel", ""), serviceLevelDescription: get(rate, "serviceLevelDescription", "") }));
            return data;
        })
    );
}

async function callLtlCarrierApi(payload) {
    const axios = require("axios");
    let data = JSON.stringify(payload);
    let config = {
        method: "post",
        maxBodyLength: Infinity,
        url: "https://dev-api.omnilogistics.com/v1/shipment/ltlrate",
        headers: {
            "Content-Type": "application/json",
            "x-api-key": "CpC6AEaO6earvSLEAGRgm4ph3jPFj0eU6IUzIQX7",
        },
        data: data,
    };

    const res = await axios.request(config);
    return res.data;
}

function sliceArrayIntoChunks(array, limit, offset) {
    const endIndex = offset + limit;
    const startIndex = endIndex - limit;
    return array.slice(startIndex, endIndex);
}

async function uploadToS3(body, key) {
    await putObject(body, key, "omni-dw-api-services-ltl-batch-rating-bucket-dev", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
}

async function sendMailWithoutAttachment({ fromEmail, toEmail, html, subject }) {
    try {
        const boundary = `----=_Part${Math.random().toString().substr(2)}`;
        const rawMessage = [
            `From: ${fromEmail}`,
            `To: ${toEmail}`,
            `Subject: ${subject}`,
            `MIME-Version: 1.0`,
            `Content-Type: multipart/alternative; boundary="${boundary}"`, // For sending both plaintext & html content
            `\n`,
            `--${boundary}`,
            `Content-Type: text/html; charset=utf-8`,
            `Content-Transfer-Encoding: 7bit`,
            `\n`,
            html,
            `--${boundary}--`,
        ];
        let params = {
            Destinations: [...toEmail],
            RawMessage: { Data: rawMessage.join("\n") },
        };
        return await ses.sendRawEmail(params).promise();
    } catch (error) {
        console.log("error", error);
        throw new Error("Email send failed");
    }
}

async function getS3PresignedUrl(bucket, object) {
    const params = {
        Bucket: bucket,
        Key: object,
        Expires: 6000,
    };
    console.info(`🙂 -> file: ltl_batch_rating.js:69 -> params:`, params);
    try {
        return await s3.getSignedUrlPromise("getObject", params);
    } catch (error) {
        console.error(`🙂 -> file: ltl_batch_rating.js:74 -> error:`, error);
        return false;
    }
}

async function deleteSqsMessage(receiptHandle) {
    console.info(`🙂 -> file: index.js:271 -> receiptHandle:`, receiptHandle);
    console.info(`🙂 -> file: index.js:277 -> process.env.BATCH_RATING_QUEUE:`, process.env.BATCH_RATING_QUEUE);
    try {
        if (receiptHandle)
            await sqs
                .deleteMessage({
                    QueueUrl: process.env.BATCH_RATING_QUEUE,
                    ReceiptHandle: receiptHandle,
                })
                .promise();
        return true;
    } catch (error) {
        console.info(`🙂 -> file: index.js:279 -> error:`, error);
        return false;
    }
}
