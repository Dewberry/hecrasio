# instantite queue objects
job_queue='dewberry-post-process.fifo'
sqs = boto3.resource("sqs")
queue = sqs.get_queue_by_name(QueueName=job_queue)

# check for items in queue 
jobs_remaining = int(queue.attributes['ApproximateNumberOfMessages'])

# Add While-True loop, scheduler for managing multiple processes
for message in queue.receive_messages():
    job_start = datetime.now()
    dtm = job_start.strftime('%Y-%m-%d %H:%M')
    jobID = message.body
    message.delete()
    # call PostProcessor, pipe output to [jobID].out file
    # upload [jobID].out file to s3
    

