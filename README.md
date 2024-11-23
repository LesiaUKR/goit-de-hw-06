# HW | Spark Streaming

Hello! How are you feeling? 😊 I hope you're excited about this new challenge!

Welcome to your homework assignment, where you'll enhance your practical skills in using Apache Kafka.

The task is similar to what you did in the previous module but much more interesting this time! 😊
Today, you'll complete a typical IoT device data monitoring task.

In this homework, you need to process a data stream from a Kafka topic generated by sensors, create a program to analyze the data, and write alerts to an alert-Kafka topic when certain conditions are met.

Alert: From the English word "alert," meaning "warning." Data that exceeds acceptable limits is considered an alert. For example, when temperature or humidity goes beyond permissible values.

## Step-by-step Instructions:
1. Generate a data stream:
Input data is the same as in the previous homework — data from a Kafka topic. Generate a data stream containing id, temperature, humidity, and timestamp. You can reuse the script and topic from the previous task.
2. Aggregate the data:
Read the data stream generated in step 1. Use a sliding window with the following settings:
- Window length: 1 minute
- Sliding interval: 30 seconds
- Watermark duration: 10 seconds
- Compute the average temperature and humidity values.
3. Define alert parameters:
Your manager likes to change the alert criteria frequently. To avoid redeploying the code each time, alert parameters are provided in a file:
alerts_conditions.csv.
The file includes the maximum and minimum values for temperature and humidity, along with the alert message and alert code. Values of -999,-999 indicate that these parameters are not used for that specific alert.
Check the file — its structure should be intuitive. You need to read the file and use its data for configuring alerts.
4. Build the alert mechanism:
Once you've computed the average values, check if they meet the criteria specified in the file. (Hint: Perform a cross join and filter the data.)
5. Write the data to a Kafka topic:
Write the generated alerts to the output Kafka topic.

An example message for Kafka as a result of your program:

```json
{
  "id": "sensor-001",
  "alert_code": "TEMP_HIGH",
  "alert_message": "Temperature exceeded the upper limit",
  "average_temperature": 78.5,
  "average_humidity": 45.2,
  "timestamp": "2024-11-23T15:30:00Z"
}
```

## Acceptance and Evaluation Criteria:
☝🏻 The following acceptance criteria are mandatory for the mentor to review your homework. If any criteria are not met, the mentor will return the task for revision without grading. If you're "just stuck"😉 or need clarification, contact your mentor on Slack.

1. Generating sensor data.
2. Computing average values of metrics.
3. Implementing a filtering mechanism to identify alerts.
4. Writing data to the Kafka topic.