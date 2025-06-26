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

# ------------------ Configuration ------------------

logging.basicConfig(level=logging.INFO)
app = Flask(__name__)
PORT = int(os.getenv('PORT', 7001))

UPLOAD_DIR = os.getenv('UPLOAD_DIR', '/app/uploads')
RESULT_DIR = os.getenv('RESULT_DIR', 'results')
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(RESULT_DIR, exist_ok=True)

REDIS_HOST = os.getenv('REDIS_HOST', 'redis')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
PUB_SUB_CHANNEL = 'task_completed'

PROCESSING_TOPICS_KEY = "processingTopics"
PROCESSED_TOPICS_KEY = "processedTopics"
topics = {}

# ------------------ Redis Setup ------------------

try:
    redis_client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=0,
        decode_responses=True
    )
    redis_client.ping()
    logging.info("✅ Connected to Redis at %s:%d", REDIS_HOST, REDIS_PORT)
except redis.exceptions.ConnectionError as e:
    logging.error("❌ Redis connection failed: %s", str(e))
    raise

# ------------------ ESRGAN Model Setup ------------------

try:
    model_path = 'models/RRDB_ESRGAN_x4.pth'
    device = torch.device('cpu')
    model = arch.RRDBNet(3, 3, 64, 23, gc=32)
    model.load_state_dict(torch.load(model_path, map_location=device), strict=True)
    model.eval()
    model = model.to(device)
    logging.info("✅ ESRGAN model loaded from: %s", model_path)
except Exception as e:
    logging.error("❌ Failed to load ESRGAN model: %s", str(e))
    raise

# ------------------ Image Processing ------------------

def process_image(image_path, topic_id):
    logging.info(f"[{topic_id}] ▶️ Processing image: {image_path}")
    topics[topic_id] = {"status": "processing", "progress": 0}

    time.sleep(2)  # Simulated delay

    img = cv2.imread(image_path, cv2.IMREAD_COLOR)
    if img is None:
        logging.error(f"[{topic_id}] ❌ Failed to read image: {image_path}")
        topics[topic_id] = {"status": "failed", "error": "Unable to read image."}
        return

    img = img * 1.0 / 255
    img = torch.from_numpy(np.transpose(img[:, :, [2, 1, 0]], (2, 0, 1))).float().unsqueeze(0).to(device)

    try:
        with torch.no_grad():
            output = model(img).data.squeeze().float().cpu().clamp_(0, 1).numpy()
    except Exception as e:
        logging.error(f"[{topic_id}] ❌ Model inference error: {e}")
        topics[topic_id] = {"status": "failed", "error": "Model inference failed"}
        return

    output = np.transpose(output[[2, 1, 0], :, :], (1, 2, 0))
    output = (output * 255.0).round()

# ------------------ Flask Startup ------------------

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

    # Download image
    try:
        response = requests.get(image_url, timeout=10)
        if response.status_code != 200:
            return jsonify({"error": f"Failed to download image, status={response.status_code}"}), 400

        with open(image_path, 'wb') as f:
            f.write(response.content)
        logging.info(f"[{topic_name}] ✅ Image downloaded to: {image_path}")
    except Exception as e:
        logging.error(f"[{topic_name}] ❌ Failed to download image: {e}")
        return jsonify({"error": "Image download failed", "details": str(e)}), 400

    topic_id = str(len(topics) + 1)
    topics[topic_id] = {
        "status": "created",
        "topicName": topic_name,
        "imageURL": image_url,
        "imagePath": image_path
    }

    threading.Thread(target=process_image, args=(image_path, topic_id)).start()
    return jsonify({"topic_id": topic_id}), 201

@app.route('/get_topic/<topic_id>', methods=['GET'])
def get_topic(topic_id):
    return jsonify(topics.get(topic_id, {"error": "Topic not found"})), 200 if topic_id in topics else 404

@app.route('/close_topic/<topic_id>', methods=['POST'])
def close_topic(topic_id):
    if topic_id in topics:
        del topics[topic_id]
        return jsonify({"message": "Topic closed"}), 200
    return jsonify({"error": "Topic not found"}), 404

# Run Flask app
if __name__ == '__main__':
    print("🚀 Starting ESRGAN Flask server...")
    app.run(host='0.0.0.0', port=PORT)
