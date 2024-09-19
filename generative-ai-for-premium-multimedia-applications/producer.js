const express = require('express');
const cors = require('cors'); // Import CORS
const { Kafka } = require('kafkajs');
const path = require('path');
const bodyParser = require('body-parser');

const kafka = new Kafka({
    clientId: 'transcoding-service',
    brokers: ['127.0.0.1:9092'] // Replace with your Kafka broker address
});

const producer = kafka.producer();
const app = express();
const PORT = 3000;

app.use(express.static(path.join(__dirname, 'public')));

// Enable CORS for all routes
app.use(cors());

app.use(bodyParser.json());

const startProducer = async () => {
    try {
        await producer.connect();
        console.log('Connect producer once at the start');
    } catch (error) {
        console.error('Error connecting Kafka producer:', error);
    }
};

startProducer();

// Kafka producer logic
const submitTask = async (topicName) => {
    try {
        await producer.send({
            topic: 'media-transcoding',
            messages: [
                { value: topicName },
            ],
        });
        console.log(`Task ${topicName} submitted to broker`);
    } catch (error) {
        console.error('Error submitting task:', error);
    }
};

// 
app.post('/submit-task', async (req, res) => {
    const { topicName } = req.body;
    console.log(`API to accept user submitted tasks topicName: ${topicName}`);
    
    try {
        await submitTask(topicName);
        res.status(200).json({ message: 'Task submitted successfully', topicName });
    } catch (error) {
        console.error('Error submitting task:', error);
        res.status(500).json({ message: 'Error submitting task', error });
    }
});

// 
const shutdownProducer = async () => {
    console.log(`Shutdown handler to disconnect producer on exit`);
    try {
        await producer.disconnect();
        console.log('Producer is disconnected');
    } catch (error) {
        console.error('Error disconnecting Kafka producer:', error);
    } finally {
        // Exit the process once the producer is disconnected
        process.exit(0); // 0 indicates a successful shutdown
    }
};

process.on('SIGINT', shutdownProducer);
process.on('SIGTERM', shutdownProducer);

app.listen(PORT, async () => {
    console.log(`Producer server running on port ${PORT}`);
});
