const { Kafka } = require('kafkajs');

const kafka = new Kafka({
    clientId: 'transcoding-service',
    brokers: ['localhost:9092'] // Replace with your Kafka broker address
});

const producer = kafka.producer();

const submitTask = async (topicName) => {
    await producer.connect();
    await producer.send({
        topic: 'media-transcoding',
        messages: [
            { value: topicName },
        ],
    });
    console.log(`Task submitted: ${topicName}`);
    await producer.disconnect();
};

submitTask('video-1.mp4').catch(console.error);
submitTask('video-2.mp4').catch(console.error);
