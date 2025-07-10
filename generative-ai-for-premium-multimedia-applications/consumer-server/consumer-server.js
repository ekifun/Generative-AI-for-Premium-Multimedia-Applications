const express = require('express');
const cors = require('cors');
const { Kafka } = require('kafkajs');
const axios = require('axios');
const path = require('path');
const redis = require('redis');

// === Constants ===
const PORT = 5001;
const KAFKA_BROKERS = ['kafka:9092'];
const TOPIC = 'media-transcoding';
const GROUP_ID = 'transcoding-group';
const ESRGANServerUrl = 'http://esrgan:7001';
const PROCESSING_TOPICS_KEY = 'processingTopics';
const PROCESSED_TOPICS_KEY = 'processedTopics';
const PUB_SUB_CHANNEL = 'task_completed';

// === Express App Setup ===
const app = express();
app.use(cors());
app.use(express.static(path.join(__dirname, 'public')));

// === Kafka Setup ===
const kafka = new Kafka({ clientId: 'transcoding-service', brokers: KAFKA_BROKERS });
const consumer = kafka.consumer({ groupId: GROUP_ID });

// === Redis Setup ===
const redisClient = redis.createClient({ url: 'redis://redis:6379' });
const pubSubClient = redis.createClient({ url: 'redis://redis:6379' });

redisClient.on('error', (err) => console.error('Redis error:', err));
pubSubClient.on('error', (err) => console.error('Redis Pub/Sub error:', err));

async function connectRedisClients() {
    await redisClient.connect();
    await pubSubClient.connect();
    console.log('✅ Connected to Redis');
}

async function getTopicsFromRedis() {
    const processed = await redisClient.lRange(PROCESSED_TOPICS_KEY, 0, -1);
    const processing = await redisClient.hGetAll(PROCESSING_TOPICS_KEY);
    return {
        processed: processed.map(topic => JSON.parse(topic)),
        processing: Object.keys(processing).map(key => ({
            name: key,
            progress: processing[key],
        }))
    };
}

// === Kafka Message Processor ===
async function processTopic(topic, imageURL) {
    await redisClient.hSet(PROCESSING_TOPICS_KEY, topic, 0);
    console.log(`Consumer consumes a topic: ${topic} with imageURL: ${imageURL}`);

    try {
        // Pass imageURL to ESRGAN microservice
        const response = await axios.post(`${ESRGANServerUrl}/create_topic`, { topicName: topic, imageURL });

        if (response.status === 201) {
            const topicId = response.data.topic_id;
            await redisClient.hSet(PROCESSING_TOPICS_KEY, topicId, 0);
            console.log(`ESRGAN microservice started to process topic: ${topic} (id: ${topicId}) with imageURL: ${imageURL}`);
        } else {
            console.log(`Failed to create task for topic: ${topic}`);
        }
    } catch (error) {
        console.error(`Error creating task for topic: ${topic}`, error);
    }

    const { processed, processing } = await getTopicsFromRedis();
    console.log(`Current processing topics: ${JSON.stringify(processing)}, processed topics: ${JSON.stringify(processed)}`);
}

// === Kafka Consumer Runner ===
async function runConsumer() {
    await consumer.connect();
    await consumer.subscribe({ topic: TOPIC, fromBeginning: true });

    await consumer.run({
        eachMessage: async ({ message }) => {
            const payload = JSON.parse(message.value.toString());
            const { topicName, imageURL } = payload;
            await processTopic(topicName, imageURL);
        },
    });    

    console.log(`✅ Kafka consumer subscribed to topic "${TOPIC}"`);
}

// === Redis Pub/Sub Listener ===
async function subscribeToTaskCompletion() {
    await pubSubClient.subscribe(PUB_SUB_CHANNEL, async (message) => {
        const { topic_id, result } = JSON.parse(message);

        await redisClient.hDel(PROCESSING_TOPICS_KEY, topic_id);
        console.log(`✅ Removed topic ${topic_id} from processing`);

        const processedTopic = JSON.stringify({ topic_id, result });
        await redisClient.rPush(PROCESSED_TOPICS_KEY, processedTopic);
        console.log(`📦 Task completed: topic=${topic_id}, result=${result}`);
    });

    console.log(`📡 Subscribed to Redis channel: ${PUB_SUB_CHANNEL}`);
}

// === Start Server ===
app.listen(PORT, async () => {
    console.log(`🚀 Consumer server running at http://localhost:${PORT}`);

    try {
        await connectRedisClients();
        await runConsumer();
        await subscribeToTaskCompletion();
    } catch (err) {
        console.error('❌ Startup error:', err);
        process.exit(1);
    }
});
