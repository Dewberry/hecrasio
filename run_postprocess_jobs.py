# instantite queue objects
job_queue='dewberry-post-process.fifo'
sqs = boto3.resource("sqs")
queue = sqs.get_queue_by_name(QueueName=job_queue)

# check for items in queue 
jobs_remaining = int(queue.attributes['ApproximateNumberOfMessages'])

for message in queue.receive_messages():
    job_start = datetime.now()
    dtm = job_start.strftime('%Y-%m-%d %H:%M')
    jobID = message.body
    print(jobID)
    # run process
    message.delete()


# In[ ]:


jobID = 'DC_F01_NBR_E0006'
projID = jobID[0:6]