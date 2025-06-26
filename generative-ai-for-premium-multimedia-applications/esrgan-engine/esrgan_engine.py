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
import requests
from urllib.parse import urlparse

# Configure logging
logging.basicConfig(level=logging.INFO)

# Initialize Flask app
app = Flask(__name__)
PORT = int(os.getenv('PORT', 7001))

# Upload and results directories (ensure Docker volumes are mounted correctly)
UPLOAD_DIR = os.getenv('UPLOAD_DIR', '/app/uploads')
RESULT_DIR = os.getenv('RESULT_DIR', 'results')
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(RESULT_DIR, exist_ok=True)

# Redis configuration
redis_client = redis.Redis(host='redis', port=6379)

PROCESSING_TOPICS_KEY = "processingTopics"
PROCESSED_TOPICS_KEY = "processedTopics"
PUB_SUB_CHANNEL = 'task_completed'

# Topic status store
topics = {}

# ESRGAN model setup
model_path = 'models/RRDB_ESRGAN_x4.pth'
device = torch.device('cpu')
model = arch.RRDBNet(3, 3, 64, 23, gc=32)
model.load_state_dict(torch.load(model_path, map_location=device), strict=True)
model.eval()
model = model.to(device)

def process_image(image_path, topic_id):
    logging.info(f"[{topic_id}] Processing image: {image_path}")
    topics[topic_id] = {"status": "processing", "progress": 0}

    time.sleep(2)  # Simulated delay

    img = cv2.imread(image_path, cv2.IMREAD_COLOR)
    if img is None:
        logging.error(f"[{topic_id}] ❌ Failed to read image: {image_path}")
        topics[topic_id] = {"status": "failed", "error": "Unable to read image."}
        return

    img = img * 1.0 / 255
    img = torch.from_numpy(np.transpose(img[:, :, [2, 1, 0]], (2, 0, 1))).float()
    img_LR = img.unsqueeze(0).to(device)

    with torch.no_grad():
        output = model(img_LR).data.squeeze().float().cpu().clamp_(0, 1).numpy()

    output = np.transpose(output[[2, 1, 0], :, :], (1, 2, 0))
    output = (output * 255.0).round()
    result_path = os.path.join(RESULT_DIR, f'{os.path.splitext(os.path.basename(image_path))[0]}_rlt.png')
    cv2.imwrite(result_path, output)

    logging.info(f"[{topic_id}] ✅ Output saved to: {result_path}")

    redis_client.publish(PUB_SUB_CHANNEL, json.dumps({
        "topic_id": topic_id,
        "status": "processed",
        "result": result_path
    }))
    topics[topic_id] = {"status": "processed", "progress": 100, "result": result_path}

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

    # Download the image from the URL
    try:
        response = requests.get(image_url, timeout=10)
        if response.status_code != 200:
            return jsonify({"error": f"Failed to download image: status={response.status_code}"}), 400

        with open(image_path, 'wb') as f:
            f.write(response.content)

        logging.info(f"[create_topic] ✅ Downloaded image to: {image_path}")
    except Exception as e:
        logging.error(f"[create_topic] ❌ Exception while downloading image: {e}")
        return jsonify({"error": "Image download failed", "details": str(e)}), 400

    topic_id = str(len(topics) + 1)
    topics[topic_id] = {
        "status": "created",
        "topicName": topic_name,
        "imageURL": image_url,
        "imagePath": image_path
    }

    logging.info(f"[create_topic] Starting thread for: {image_path}")
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
