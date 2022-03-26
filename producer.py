# producer.py

from ensurepip import bootstrap
import time
import cv2
from kafka import KafkaProducer
# connect to kafka
producer = KafkaProducer(bootstrap_servers=['0.0.0.0:9092'])
# Assign a topic
topic = 'my-topic'

def video_emitter(video):
    # Open the video file
    video = cv2.VideoCapture(video)
    print(' emitting.....')

    # read the file
    while (video.isOpened):
        # read the image in each frame
        success, image = video.read()
        # Check if the file has been read to the end
        if not success:
            break
        # Convert image to png
        ret, jpeg = cv2.imencode('.png', image)
        # Convert image to bytes and send over kafka
        producer.send(topic, jpeg.tobytes())
        # Sleep to curtail CPU usage
        time.sleep(0.2)
    # Clear the capture
    video.release()
    print('done emitting')

if __name__ == '__main__':
    video_emitter('video.mp4')