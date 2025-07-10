# Generative-AI-for-Premium-Multimedia-Applications

🌐 [Live Demo](https://ekifun.github.io/Generative-AI-for-Premium-Multimedia-Applications/)  
📂 [Source Code](https://github.com/ekifun/Generative-AI-for-Premium-Multimedia-Applications)

### 1. Frontend (index.html):
- Displays the processed and processing topics (media tasks).
- Allows users to submit new tasks through a form.
### 2. Backend (Producer.js):
- Handles task submissions via HTTP requests.
- Uses Kafka to send the submitted tasks to a "media-upscaling" topic for processing.
### 3. Consumer Service (consumer-server.js):
- Consumes tasks from the Kafka topic and initiates processing using the ESRGAN model.
- Interacts with a Redis database to keep track of processed and processing topics.
- Sends task creation requests to the ESRGAN microservice and queries the task status regularly to check for completion.
### 4. ESRGAN Microservice (ESRGAN_microservice.py):
- Receives task creation requests and processes media (images in this case) using the ESRGAN model for super-resolution.
- Returns the status of the task (processing or processed).
- Allows cleanup by closing completed topics.
### 5. Redis:
- Stores the state of processed and processing tasks for persistent tracking across the system.
