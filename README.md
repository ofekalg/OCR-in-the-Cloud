# OCR in the Cloud

This project was given to me as an assignment as part of a course during my computer science bechelor dgree. In this project I used AWS services, such as: EC2, S3 and SQS, to work with large amount of data.

In this assignment I coded a real-world application to distributively apply OCR algorithms on images. The result of this program displays each image with its recognized text on a webpage.

The application is composed of a local application and instances running on the Amazon cloud. The application will get as an input a text file containing a list of URLs of images. Then, instances will be launched in AWS (workers). Each worker will download image files, use some OCR library to identify text in those images (if any) and display the image with the text in a webpage.

The OCR tool we used is Tesseract (installed only on the workers).

# The Application Flow

<img width="566" alt="Application_Flow" src="https://user-images.githubusercontent.com/44983890/117695986-7e462080-b1c9-11eb-9437-6e71f366e214.png">

  1. Local Application uploads the file with the list of images to S3
  2. Local Application sends a message (queue) stating of the location of the images list on S3
  3. Local Application does one of the two:<br>
      - Starts the manager<br>
      - Checks if a manager is active and if not, starts it<br>
  4. Manager downloads list of images
  5. Manager creates an SQS message for each URL in the list of images
  6. Manager bootstraps nodes to process messages
  7. Worker gets an image message from an SQS queue
  8. Worker downloads the image indicated in the message
  9. Worker applies OCR on image.
  10. Worker puts a message in an SQS queue indicating the original URL of the image and the text.
  11. Manager reads all the Workers' messages from SQS and creates one summary file
  12. Manager uploads summary file to S3
  13. Manager posts an SQS message about summary file
  14. Local Application reads SQS message
  15. Local Application downloads summary file from S3
  16. Local Application creates html output files
