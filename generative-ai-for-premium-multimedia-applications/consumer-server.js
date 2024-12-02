const express = require('express');
const cors = require('cors');
const { Kafka } = require('kafkajs');
const axios = require('axios');
const path = require('path');
const redis = require('redis');

const app = express();
const PORT = 5001;

// Serve static files (HTML, CSS, JS) from the "public" directory
app.use(express.static(path.join(__dirname, 'public')));

// Enable CORS for all routes
app.use(cors());

// URL of the microservice model server
const ESRGANServerUrl = 'http://127.0.0.1:8080'; // Update with the actual URL of the model server

// Initialize Kafka client and topics
const kafka = new Kafka({
    clientId: 'transcoding-service',
    brokers: ['127.0.0.1:9092'] // Replace with your Kafka broker address
});

// Kafka consumer setup
const consumer = kafka.consumer({ groupId: 'transcoding-group' });

// Redis client setup
const redisClient = redis.createClient();
const pubSubClient = redisClient.duplicate(); // Duplicate Redis client for Pub/Sub

const PROCESSING_TOPICS_KEY = 'processingTopics';
const PROCESSED_TOPICS_KEY = 'processedTopics';
const PUB_SUB_CHANNEL = 'task_completed';

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

// Simulating topic processing
async function processTopic(topic, imageURL) {
    // Add the topic to Redis under processing topics with initial progress 0
    await redisClient.hSet(PROCESSING_TOPICS_KEY, topic, 0);
    console.log(`Consumer consumes a topic: ${topic}`);

    try {
        // Send a request to create a new task of super-resolution using generative AI model
        console.log(`Sending POST request to ESRGAN AI model to create a new topic: ${topic}`);
        const response = await axios.post(`${ESRGANServerUrl}/create_topic`, {
            topicName: topic,
            imageURL: imageURL
        });

        if (response.status === 201) {
            const topicId = response.data.topic_id;
            redisClient.hSet(PROCESSING_TOPICS_KEY, topicId, 0)
            console.log(`Set topic ${topicId} to 'processing' in Redis.`);

            console.log(`ESRGAN microservice started to processing the request, topic Id: ${topicId} for topic: ${topic}`);
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
            const { topicName, imageURL } = JSON.parse(message.value.toString());
            processTopic(topicName, imageURL);
        },
    });
};

// Redis Pub/Sub subscriber to listen for task completion
async function subscribeToTaskCompletion() {
    await pubSubClient.connect(); // Ensure connection to Redis Pub/Sub

    await pubSubClient.subscribe(PUB_SUB_CHANNEL, async (message) => {
        const { topic_id, result } = JSON.parse(message);

        // Update Redis to reflect task completion
      redisClient.hDel(PROCESSING_TOPICS_KEY, topic_id);
      console.log(`Removed topic ${topic_id} from processing list in Redis.`);

      // Create the processed topic object in the desired format
      const processedTopic = JSON.stringify({
        topic_id: topic_id,
        result: result
      });

      redisClient.rPush(PROCESSED_TOPICS_KEY, processedTopic);
      console.log(`Pushed processed topic to processed list in Redis: ${processedTopic}`);

      console.log(`Task completed for topic: ${topic_id}. Result: ${result}`);
    });

    console.log(`Subscribed to Redis Pub/Sub channel: ${PUB_SUB_CHANNEL}`);
}

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

    runConsumer().catch(console.error);
    subscribeToTaskCompletion().catch(console.error);
});

