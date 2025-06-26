const express = require('express');
const cors = require('cors');
const { Kafka } = require('kafkajs');
const path = require('path');
const bodyParser = require('body-parser');
const redis = require('redis');

// === Express App Setup ===
const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.static(path.join(__dirname, 'public')));
app.use(cors());
app.use(bodyParser.json());

// === Kafka Setup ===
const kafka = new Kafka({
    clientId: 'producer-service',
    brokers: ['kafka:9092']  // ✅ MUST be this inside Docker network
  });  

const producer = kafka.producer();

// === Redis Setup ===
const redisClient = redis.createClient({
    socket: {
        host: 'redis',   // use Docker service name, NOT 'localhost'
        port: 6379
    }
});

const PROCESSING_TOPICS_KEY = 'processingTopics';
const PROCESSED_TOPICS_KEY = 'processedTopics';

redisClient.on('error', (err) => {
    console.error('Redis error:', err);
});

redisClient.connect()
    .then(() => console.log('✅ Connected to Redis'))
    .catch(err => {
        console.error('❌ Could not connect to Redis:', err);
        process.exit(1);
    });


// === Kafka Producer Start ===
const startProducer = async () => {
    try {
        await producer.connect();
        console.log('Kafka producer connected');
    } catch (error) {
        console.error('Error connecting Kafka producer:', error);
    }
};

startProducer();

// === Task Submission to Kafka ===
const submitTask = async (topicName) => {
    try {
        await producer.send({
            topic: 'media-transcoding',
            messages: [{ value: topicName }],
        });
        console.log(`✅ Task "${topicName}" submitted to Kafka`);
    } catch (error) {
        console.error('❌ Error submitting task:', error);
    }
};

app.post('/submit-topic', async (req, res) => {
    const { topicName, imageURL } = req.body;
    console.log(`API to accept user submitted tasks: topicName=${topicName}, imageURL=${imageURL}`);
    
    try {
        await producer.send({
            topic: 'media-transcoding',
            messages: [
                { value: JSON.stringify({ topicName, imageURL }) }, // Send as JSON
            ],
        });
        res.status(200).json({ message: 'Task submitted successfully', topicName, imageURL });
    } catch (error) {
        console.error('Error submitting task:', error);
        res.status(500).json({ message: 'Error submitting task', error });
    }
});

// === /get-status API ===
async function getTopicsFromRedis() {
    const processed = await redisClient.lRange(PROCESSED_TOPICS_KEY, 0, -1);
    const processing = await redisClient.hGetAll(PROCESSING_TOPICS_KEY);

    return {
        processed: processed.map((topicStr) => {
            try {
                return JSON.parse(topicStr);
            } catch {
                return { name: topicStr };
            }
        }),
        processing: Object.keys(processing).map(key => ({
            name: key,
            progress: processing[key]
        }))
    };
}

app.get('/get-status', async (req, res) => {
    try {
        const { processed, processing } = await getTopicsFromRedis();
        res.json({ processed, processing });
    } catch (error) {
        console.error('❌ Error fetching status:', error);
        res.status(500).json({ message: 'Error fetching status', error });
    }
});

app.get('/', (req, res) => {
    res.send('Producer service is running.');
});

// === Shutdown Handler ===
const shutdownProducer = async () => {
    console.log('🔌 Shutting down Kafka producer...');
    try {
        await producer.disconnect();
        console.log('✅ Kafka producer disconnected');
    } catch (error) {
        console.error('❌ Error during producer disconnect:', error);
    } finally {
        process.exit(0);
    }
};

process.on('SIGINT', shutdownProducer);
process.on('SIGTERM', shutdownProducer);

// === Start Server ===
app.listen(PORT, () => {
    console.log(`🚀 Producer server running on http://localhost:${PORT}`);
});
