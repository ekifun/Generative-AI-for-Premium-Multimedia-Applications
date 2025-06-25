from flask import Flask, request, jsonify
import os
import torch
import cv2
import numpy as np
import json
import redis
import RRDBNet_arch as arch
import threading
import time
import logging
from urllib.parse import urlparse

# Configure logging
logging.basicConfig(level=logging.INFO)

# Initialize Flask app
app = Flask(__name__)
PORT = int(os.getenv('PORT', 7001))
UPLOAD_DIR = os.getenv('UPLOAD_DIR', '/app/uploads')  # Folder mounted in Docker

# Initialize Redis client (use Docker hostname for Redis)
redis_client = redis.Redis(
    host='redis',
    port=6379,
    db=0,
    decode_responses=True
)

PROCESSING_TOPICS_KEY = "processingTopics"
PROCESSED_TOPICS_KEY = "processedTopics"
PUB_SUB_CHANNEL = 'task_completed'

# In-memory topic status store
topics = {}

# Model setup
model_path = 'models/RRDB_ESRGAN_x4.pth'
device = torch.device('cpu')
model = arch.RRDBNet(3, 3, 64, 23, gc=32)
model.load_state_dict(torch.load(model_path, map_location=device), strict=True)
model.eval()
model = model.to(device)

def process_image(image_path, topic_id):
    logging.info(f"[{topic_id}] Processing image: {image_path}")
    topics[topic_id] = {"status": "processing", "progress": 0}

    time.sleep(2)  # Simulate delay

    img = cv2.imread(image_path, cv2.IMREAD_COLOR)
    img = img * 1.0 / 255
    img = torch.from_numpy(np.transpose(img[:, :, [2, 1, 0]], (2, 0, 1))).float()
    img_LR = img.unsqueeze(0).to(device)

    with torch.no_grad():
        output = model(img_LR).data.squeeze().float().cpu().clamp_(0, 1).numpy()

    output = np.transpose(output[[2, 1, 0], :, :], (1, 2, 0))
    output = (output * 255.0).round()
    output_path = f"results/{os.path.splitext(os.path.basename(image_path))[0]}_rlt.png"

    cv2.imwrite(output_path, output)
    logging.info(f"[{topic_id}] Output saved: {output_path}")

    redis_client.publish(PUB_SUB_CHANNEL, json.dumps({
        "topic_id": topic_id,
        "status": "processed",
        "result": output_path
    }))
    topics[topic_id] = {
        "status": "processed",
        "progress": 100,
        "result": output_path
    }

@app.route('/create_topic', methods=['POST'])
def create_topic():
    data = request.json
    topic_name = data.get("topicName")
    image_url = data.get("imageURL")

    if not topic_name or not image_url:
        return jsonify({"error": "Both topicName and imageURL are required."}), 400

    parsed_url = urlparse(image_url)
    filename = os.path.basename(parsed_url.path)
    image_path = os.path.join(UPLOAD_DIR, filename)

    topic_id = str(len(topics) + 1)
    topics[topic_id] = {
        "status": "created",
        "topicName": topic_name,
        "imageURL": image_url
    }

    logging.info(f"[{topic_id}] Starting async processing for image: {image_path}")
    threading.Thread(target=process_image, args=(image_path, topic_id)).start()

    return jsonify({"topic_id": topic_id}), 201

@app.route('/get_topic/<topic_id>', methods=['GET'])
def get_topic(topic_id):
    if topic_id in topics:
        return jsonify(topics[topic_id]), 200
    return jsonify({"error": "Topic not found"}), 404

@app.route('/close_topic/<topic_id>', methods=['POST'])
def close_topic(topic_id):
    if topic_id in topics:
        del topics[topic_id]
        return jsonify({"message": "Topic closed"}), 200
    return jsonify({"error": "Topic not found"}), 404

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=PORT)
