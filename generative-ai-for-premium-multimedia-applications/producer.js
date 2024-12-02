const express = require('express');
const cors = require('cors'); // Import CORS
const { Kafka } = require('kafkajs');
const path = require('path');
const bodyParser = require('body-parser');
const multer = require("multer");

const kafka = new Kafka({
    clientId: 'transcoding-service',
    brokers: ['127.0.0.1:9092'] // Replace with your Kafka broker address
});

const producer = kafka.producer();
const app = express();
const PORT = process.env.PORT || 3000;

let fileInfo;

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
const submitTask = async (topicName, inputName) => {
    try {
        imageURL = `http://127.0.0.1:${PORT}/uploads/${fileInfo}`;
        console.log(`imageURL: ${imageURL}`);
        const messagePayload = JSON.stringify({ topicName, imageURL });
        await producer.send({
            topic: 'media-transcoding',
            messages: [
                { value: messagePayload },
            ],
        });
        console.log(`Task ${topicName} submitted to broker`);
    } catch (error) {
        console.error('Error submitting task:', error);
    }
};

// 
app.post('/submit-topic', async (req, res) => {
    const { topicName, inputName} = req.body;
    console.log(`API to accept user submitted tasks topicName: ${topicName}`);
    
    try {
        await submitTask(topicName, inputName);
        res.status(200).json({ message: 'Task submitted successfully', topicName });
    } catch (error) {
        console.error('Error submitting task:', error);
        res.status(500).json({ message: 'Error submitting task', error });
    }
});

// Configure Multer for file uploads
const storage = multer.diskStorage({
    destination: (req, file, cb) => {
      cb(null, "uploads/"); // Directory to save uploaded files
    },
    filename: (req, file, cb) => {
      const fileName = Date.now() + path.extname(file.originalname);
      fileInfo = fileName;
      cb(null, fileName); // Unique file name
    },

    
});
  
const upload = multer({ storage });

// Serve static files (optional for testing)
app.use(express.static("public"));

// Endpoint to handle image upload
app.post("/upload-image", upload.single("image"), (req, res) => {
    if (!req.file) {
        return res.status(400).send("No file uploaded.");
    }

    //fileInfo.set(req.file.originalname, req.file);

    console.log("Uploaded file:", req.file);
    res.send(`File uploaded successfully: ${req.file.originalname}`);
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
