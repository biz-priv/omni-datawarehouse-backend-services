let limit = 20;
let offset = null;
module.exports.handler = async (event, context, callback) => {
    console.info(`🙂 -> file: index.js:4 -> event:`, event);
    if (offset === null) {
        return await startNextStep({ event, limit, offset });
    }
    if (new Date().getTime() % 2 === 0) {
        return callback(null, { hasMoreData: "true", limit, offset });
    } else {
        return callback(null, { hasMoreData: "false", limit, offset });
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
