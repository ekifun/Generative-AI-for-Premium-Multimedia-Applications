const express = require('express');
const { Kafka } = require('kafkajs');
const axios = require('axios');
const path = require('path');
const redis = require('redis'); // Import Redis

const app = express();
const PORT = 5001;

// Serve static files (HTML, CSS, JS) from the "public" directory
app.use(express.static(path.join(__dirname, 'public')));

// URL of the microservice model server
const ESRGANServerUrl = 'http://localhost:5000'; // Update with the actual URL of the model server

// Initialize Kafka client and topics
const kafka = new Kafka({
    clientId: 'transcoding-service',
    brokers: ['localhost:9092'] // Replace with your Kafka broker address
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'transcoding-group' });

// Initialize Redis client
const redisClient = redis.createClient();

// Connect to Redis
redisClient.connect().catch(console.error);

// Redis keys for storing topics
const PROCESSED_TOPICS_KEY = 'processedTopics';
const PROCESSING_TOPICS_KEY = 'processingTopics';

// Helper function to get topics from Redis
async function getTopicsFromRedis() {
    const processed = await redisClient.lRange(PROCESSED_TOPICS_KEY, 0, -1);
    const processing = await redisClient.hGetAll(PROCESSING_TOPICS_KEY);

    return {
        processed: processed.map(topic => ({ name: topic })),
        processing: Object.keys(processing).map(key => ({ name: key, progress: processing[key] }))
    };
}

// Simulating topic processing
async function processTopic(topic) {
    // Add the topic to processing topics in Redis
    await redisClient.hSet(PROCESSING_TOPICS_KEY, topic, 0);
    console.log(`Process topic: ${topic}`);

    try {
        console.log(`Sending POST request to the ESRGAN microservice to create a new topic/task, ESRGANServerUrl: ${ESRGANServerUrl}, action: create_topic`);
        const response = await axios.post(`${ESRGANServerUrl}/create_topic`, { topicName: topic });

        if (response.status === 201) {
            const topic_id = response.data.topic_id;
            console.log(`Task successfully created, topic ID: ${topic_id} for topic: ${topic}`);

            // Once done processing, update Redis by moving the topic to processed
            await redisClient.hDel(PROCESSING_TOPICS_KEY, topic);
            await redisClient.rPush(PROCESSED_TOPICS_KEY, topic);

        } else {
            console.log(`Failed to create task for topic: ${topic}`);
        }

    } catch (error) {
        console.error(`Error creating task for topic: ${topic}`, error);
    }

    // Log the current processing and processed topics
    const { processed, processing } = await getTopicsFromRedis();
    console.log('Current processing topics:', processing);
    console.log('Processed topics:', processed);
}

// Kafka consumer logic to consume topics and process them
const runConsumer = async () => {
    await consumer.connect();
    await consumer.subscribe({ topic: 'media-transcoding', fromBeginning: true });

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const topicName = message.value.toString();
            console.log(`Received topic: ${topicName}`);
            processTopic(topicName);
        },
    });
};

// Retrieve stored topics from Redis and serve to the frontend
app.get('/topics', async (req, res) => {
    const { processed, processing } = await getTopicsFromRedis();
    res.json({ processed, processing });
});

app.listen(PORT, async () => {
    console.log(`Server running on port ${PORT}`);

    // Ensure Redis is populated with existing data at startup
    const { processed, processing } = await getTopicsFromRedis();
    console.log('Restored processed topics from Redis:', processed);
    console.log('Restored processing topics from Redis:', processing);
});

runConsumer().catch(console.error);
