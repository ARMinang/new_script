import cv2
import numpy as np
import os
# run on CPU, to run on GPU comment this line or write '0'
# os.environ['CUDA_VISIBLE_DEVICES'] = '-1'
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'
import tensorflow as tf
from PIL import Image


from object_detection.utils import label_map_util


# Model preparation 
PATH_TO_FROZEN_GRAPH = 'CAPTCHA_frozen_inference_graph.pb'
# List of the strings that is used to add correct label for each box.
PATH_TO_LABELS = 'CAPTCHA_labelmap.pbtxt'
NUM_CLASSES = 24


# Load a (frozen) Tensorflow model into memory.
label_map = label_map_util.load_labelmap(PATH_TO_LABELS)
categories = label_map_util.convert_label_map_to_categories(label_map, max_num_classes=NUM_CLASSES, use_display_name=True)
category_index = label_map_util.create_category_index(categories)

detection_graph = tf.Graph()
with detection_graph.as_default():
    od_graph_def = tf.compat.v1.GraphDef()
    with tf.io.gfile.GFile(PATH_TO_FROZEN_GRAPH, 'rb') as fid:
        serialized_graph = fid.read()
        od_graph_def.ParseFromString(serialized_graph)
        tf.import_graph_def(od_graph_def, name='')


# Detection
def captcha_detection(image, average_distance_error=3):
    with detection_graph.as_default():
        with tf.compat.v1.Session(graph=detection_graph) as sess:
            # Open image
            new_image = Image.new("RGBA", image.size, "WHITE") # Create a white rgba background
            new_image.paste(image, (0, 0), image)
            image_np = np.array(new_image) 
            # Resize image if needed
            image_np = cv2.resize(image_np, (0,0), fx=3, fy=3) 
            # To get real color we do this:
            image_np = cv2.cvtColor(image_np, cv2.COLOR_BGR2RGB)
            # Expand dimensions since the model expects images to have shape: [1, None, None, 3]
            image_np_expanded = np.expand_dims(image_np, axis=0)
            # Actual detection.
            image_tensor = detection_graph.get_tensor_by_name('image_tensor:0')
            boxes = detection_graph.get_tensor_by_name('detection_boxes:0')
            scores = detection_graph.get_tensor_by_name('detection_scores:0')
            classes = detection_graph.get_tensor_by_name('detection_classes:0')
            num_detections = detection_graph.get_tensor_by_name('num_detections:0')
            # Visualization of the results of a detection.
            (boxes, scores, classes, num_detections) = sess.run(
                [boxes, scores, classes, num_detections],
                feed_dict={image_tensor: image_np_expanded}
            )

            # Bellow we do filtering stuff
            captcha_array = []
            # loop our all detection boxes
            for i,b in enumerate(boxes[0]):
                for Symbol in range(NUM_CLASSES):
                    if classes[0][i] == Symbol: # check if detected class equal to our symbols
                        if scores[0][i] >= 0.65: # do something only if detected score more han 0.65
                                            # x-left        # x-right
                            mid_x = (boxes[0][i][1]+boxes[0][i][3])/2 # find x coordinates center of letter
                            # to captcha_array array save detected Symbol, middle X coordinates and detection percentage
                            captcha_array.append([category_index[Symbol].get('name'), mid_x, scores[0][i]])

            # rearange array acording to X coordinates datected
            for number in range(20):
                for captcha_number in range(len(captcha_array)-1):
                    if captcha_array[captcha_number][1] > captcha_array[captcha_number+1][1]:
                        temporary_captcha = captcha_array[captcha_number]
                        captcha_array[captcha_number] = captcha_array[captcha_number+1]
                        captcha_array[captcha_number+1] = temporary_captcha


            # Find average distance between detected symbols
            average = 0
            captcha_len = len(captcha_array)-1
            while captcha_len > 0:
                average += captcha_array[captcha_len][1]- captcha_array[captcha_len-1][1]
                captcha_len -= 1
            # Increase average distance error
            average = average/(len(captcha_array)+average_distance_error)

            
            captcha_array_filtered = list(captcha_array)
            captcha_len = len(captcha_array)-1
            while captcha_len > 0:
                # if average distance is larger than error distance
                if captcha_array[captcha_len][1]- captcha_array[captcha_len-1][1] < average:
                    # check which symbol has higher detection percentage
                    if captcha_array[captcha_len][2] > captcha_array[captcha_len-1][2]:
                        del captcha_array_filtered[captcha_len-1]
                    else:
                        del captcha_array_filtered[captcha_len]
                captcha_len -= 1

            # Get final string from filtered CAPTCHA array
            captcha_string = ""
            for captcha_letter in range(len(captcha_array_filtered)):
                captcha_string += captcha_array_filtered[captcha_letter][0]
                
            return captcha_string