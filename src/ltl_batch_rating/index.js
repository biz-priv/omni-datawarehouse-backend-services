const AWS = require("aws-sdk");
const { get } = require("lodash");
const s3 = new AWS.S3({ region: process.env.REGION });
const XLSX = require("xlsx");
let limit = 20;
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
                    return await startNextStep({ limit, offset: 0, bucket, key });
                })
            );
        }
        const input = get(event, "input", {});
        offset = get(input, "offset", 0);
        const bucket = get(input, "bucket");
        const key = get(input, "key");

        const s3Object = await getExcel(bucket, key);
        console.info(`🙂 -> file: index.js:31 -> s3Object:`, s3Object);
        if (new Date().getTime() % 2 === 0) {
            return callback(null, { hasMoreData: "true", limit, offset, bucket, key });
        } else {
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
            input: JSON.stringify({ input }),
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
                    bucket,
                    key,
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
                    reject(false);
                })
                .on("end", async () => {
                    item = Buffer.concat(item);
                    const wb = XLSX.read(Buffer.concat(item));

                    /* generate CSV from first worksheet */
                    const first_ws = wb.Sheets[wb.SheetNames[0]];
                    const data = XLSX.utils.sheet_add_json(first_ws);
                    console.log(data);
                    resolve(data);
                });
        } catch (error) {
            console.error("Error while reading data line by line", error);
            reject(false);
        }
    });
}
