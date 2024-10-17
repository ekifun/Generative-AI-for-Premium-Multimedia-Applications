const express = require('express');
const cors = require('cors');
const { Kafka } = require('kafkajs');
const axios = require('axios');
const path = require('path');
const redis = require('redis'); // Import Redis

const app = express();
const PORT = 5001;

// Serve static files (HTML, CSS, JS) from the "public" directory
app.use(express.static(path.join(__dirname, 'public')));

// Enable CORS for all routes
app.use(cors());

// URL of the microservice model server
const ESRGANServerUrl = 'http://127.0.0.1:5000'; // Update with the actual URL of the model server

// Initialize Kafka client and topics
const kafka = new Kafka({
    clientId: 'transcoding-service',
    brokers: ['127.0.0.1:9092'] // Replace with your Kafka broker address
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'transcoding-group' });

// Initialize Redis client
const redisClient = redis.createClient();

redisClient.on('error', (err) => {
    console.error('Redis error:', err);
});

// Connect to Redis
redisClient.connect()
.then(() => {
    console.log('Connected to Redis')
})
.catch(err => {
    console.error('Could not connect to Redis:', err);
    process.exit(1); // Optionally terminate the process if Redis connection is essential
});

// Redis keys for storing topics
const PROCESSED_TOPICS_KEY = 'processedTopics';
const PROCESSING_TOPICS_KEY = 'processingTopics';
// Function to check topic status
async function queryTopicStatus(topic, topic_id, retryCount = 0) {
    const MAX_RETRIES = 5;
    const RETRY_DELAY = Math.min(10000 * (retryCount + 1), 60000); // Exponential backoff with a cap at 60 seconds
    try {
        console.log(`Send request to get the status of topic: ${topic}`);
        const response = await axios.get(`${ESRGANServerUrl}/get_topic/${topic_id}`);
        console.log(`Get the AI modle processing status: ${response.data.status}`);
        if (response.data.status === 'processed') {
            console.log(`Topic ${topic} has been processed. Closing the topic.`);
            await closeTopic(topic, topic_id);
        } else {
            console.log(`Topic: ${topic} is still processing. Retrying in 10 seconds.`);
            setTimeout(() => queryTopicStatus(topic, topic_id), RETRY_DELAY);
        }
    } catch (error) {
        console.error(`Error querying topic status for ${topic}: `, error);
        if (retryCount < MAX_RETRIES) {
            console.log(`Retrying... Attempt ${retryCount + 1}`);
            setTimeout(() => queryTopicStatus(topic, topic_id, retryCount + 1), RETRY_DELAY);
        } else {
            console.error(`Max retries reached. Close topic ${topic_id} because of error`);
        }
    }
}

// Helper function to get the current state of topics from Redis (processed and processing)
async function getTopicsFromRedis() {
    const processed = await redisClient.lRange(PROCESSED_TOPICS_KEY, 0, -1);
    const processing = await redisClient.hGetAll(PROCESSING_TOPICS_KEY);
    console.log(`getTopicsFromRedis`);
    return {
        processed: processed.map(topic => ({ name: topic })), // Convert processed topics into an object array
        processing: Object.keys(processing).map(key => ({ name: key, progress: processing[key] })) // Convert processing topics with progress
    };
}

// Function to close a topic after it's processed
async function closeTopic(topic, topic_id) {
    try {
        const response = await axios.post(`${ESRGANServerUrl}/close_topic/${topic_id}`, { topic_id });
        console.log(`Close the topic: ${topic} response.status: ${response.status}`);
        if (response.status === 200) {
            console.log(`Topic: ${topic} successfully closed.`);
            await redisClient.hDel(PROCESSING_TOPICS_KEY, topic); // Remove from processing
            await redisClient.rPush(PROCESSED_TOPICS_KEY, topic); // Add to processed list
        } else {
            console.error(`Failed to close topic: ${topic}`);
        }
    } catch (error) {
        console.error(`Error closing topic: ${topic}: `, error);
    }
}

// Simulating topic processing
async function processTopic(topic) {
    // Add the topic to Redis under processing topics with initial progress 0
    await redisClient.hSet(PROCESSING_TOPICS_KEY, topic, 0);
    console.log(`Consumer consumes a topic: ${topic}`);

    try {
        // Send a request to create a new topic/task on the model server
        console.log(`Sending POST request to ESRGAN AI model to create a new topic: ${topic}`);
        const response = await axios.post(`${ESRGANServerUrl}/create_topic`, { topicName: topic });

        if (response.status === 201) {
            const topic_id = response.data.topic_id;
            console.log(`Topic was successfully created, topic Id: ${topic_id} for topic: ${topic}`);

            // Once done processing, update Redis by moving the topic to processed
            await redisClient.hDel(PROCESSING_TOPICS_KEY, topic);
            await redisClient.rPush(PROCESSED_TOPICS_KEY, topic);

            // Start querying the status of the topic
            queryTopicStatus(topic, topic_id);
        } else {
            console.log(`Failed to create task for topic: ${topic}`);
        }

    } catch (error) {
        console.error(`Error creating task for topic: ${topic}`, error);
    }

    // Log the current processing and processed topics
    const { processed, processing } = await getTopicsFromRedis();
    console.log(`Current processing topics: ${processing}, processed topics: ${processed}`);
}

// Kafka consumer logic to consume topics and process them
const runConsumer = async () => {
    await consumer.connect();
    await consumer.subscribe({ topic: 'media-transcoding', fromBeginning: true });

    await consumer.run({
        eachMessage: async ({ message }) => {
            const topicName = message.value.toString();
            processTopic(topicName);
        },
    });
};

// Retrieve stored topics from Redis and serve to the frontend
app.get('/get-status', async (req, res) => {
    const { processed, processing } = await getTopicsFromRedis();
    res.json({ processed, processing }); // Send the topics as JSON to the frontend
});

app.listen(PORT, async () => {
    console.log(`Server running on port ${PORT}`);

    // Ensure Redis is populated with existing data at startup
    const { processed, processing } = await getTopicsFromRedis();
    console.log('Restored processed topics from Redis:', processed);
    console.log('Restored processing topics from Redis:', processing);
});

runConsumer().catch(console.error);
