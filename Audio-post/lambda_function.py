import io
import os
import csv
import json
import boto3
import array
import time
import json, datetime
 
s3 = boto3.client('s3')
sqs = boto3.client('sqs')
transcribe = boto3.client('transcribe')


def parse_transcribe_ouput(Transcribe_jsondata):
     
    rawjsondata = Transcribe_jsondata

    data_for_athena = {"time": [], "speaker_tag": [], "comment": []}

    # Identifying speaker populating speakers into an array
    if "speaker_labels" in rawjsondata["results"].keys():
        
        print("SPEAKER IDENTIFICATION PARA")

        # processing segment for building array for speaker and time duration for building csv file
        for segment in rawjsondata["results"]["speaker_labels"]["segments"]:
 
 
            
            # if items
            if len(segment["items"]) > 0:

                data_for_athena["time"].append(time_conversion(segment["start_time"]))
                timesm = time_conversion(segment["start_time"])

                
                data_for_athena["speaker_tag"].append(segment["speaker_label"])

                data_for_athena["comment"].append("")
 

                # looping thru each word 
                for word in segment["items"]:
                    
                    pronunciations = list(
                        filter(
                            lambda x: x["type"] == "pronunciation",
                            rawjsondata["results"]["items"],
                        )
                    )


 
                    word_result = list(
                        filter(
                            lambda x: x["start_time"] == word["start_time"]
                            and x["end_time"] == word["end_time"],
                            pronunciations,
                        )
                    )
 
                    result = sorted(
                        word_result[-1]["alternatives"], key=lambda x: x["confidence"]
                    )[-1]
 

                    # for the word!
                    data_for_athena["comment"][-1] += " " + result["content"]
 
                    # Check for punctuation !!!!
                    try:
                        word_result_index = rawjsondata["results"]["items"].index(
                            word_result[0]
                        )
                        next_item = rawjsondata["results"]["items"][word_result_index + 1]
                        if next_item["type"] == "punctuation":
                            data_for_athena["comment"][-1] += next_item["alternatives"][0][
                                "content"
                  
                            ]
        
                    except IndexError:
                        
                        pass
 

    # Invalid File exiting!
    else:
        print("Need to have speaker identification, Please check the file USE WAV format Audio file for better results")
        return
    


    return data_for_athena

def time_conversion(timeX):
    
    times = datetime.timedelta(seconds=float(timeX))
    times = times - datetime.timedelta(microseconds=times.microseconds)
    return str(times)



def lambda_handler(event, context):
    

    print("event: {}".format(event))
     

    BUCKET = os.environ['PROCSDBUCKET']
    AudioQ = os.environ['ASYNC_AUDIO_QUEUE_URL']
    print("Reading SQS for transcribe job submitted for audio transcription")
    print(AudioQ)
    AthenaNamedQuery = os.environ['AthenaNamedQuery']
    #OutputLocation = 's3://'+BUCKET+'/transcribe/audio_out_csv/'
    AthenResultsOutput = 's3://'+BUCKET+'/transcribe/AthenaResultsOutput/'
     

    #Reads the Transcription request from SQS 
    
    response = sqs.receive_message(
        QueueUrl=AudioQ,
        MaxNumberOfMessages=1,
        VisibilityTimeout=60 #14400
    )
    print("Message from SQS")
    print(response)

    if('Messages' in response):
        msg = response['Messages']
    else:
        print("No messages in queue, Executes in next batch schedule")
        return

    print(msg[0]['Body'])
    audio_transcribed_job = (msg[0]['Body'])
    audio_transcribed_json = (msg[0]['Body'])+".json"
    
    receipt_handle = msg[0]['ReceiptHandle']
    
    
     
    while True:
        status = transcribe.get_transcription_job(TranscriptionJobName=audio_transcribed_job)
        print('Response from get transcription Job')
        print(status)
        if status['TranscriptionJob']['TranscriptionJobStatus'] in ['COMPLETED', 'FAILED']:
            break
        print('Job Still running Executes in next batch(2Min) schedule!')
        return
    print(status)
    
    if status['TranscriptionJob']['TranscriptionJobStatus'] in ['COMPLETED']:
        # Checks the status of transcribe job and once completed it will delete the job.
        response = transcribe.delete_transcription_job(TranscriptionJobName=audio_transcribed_job)
        
    if status['TranscriptionJob']['TranscriptionJobStatus'] in ['FAILED']:
        print('Job Failed issue with Audio file please check audio file and upload proper WAV audio file again')
        response = transcribe.delete_transcription_job(TranscriptionJobName=audio_transcribed_job)
        
        sqs.delete_message(
            QueueUrl=AudioQ,
            ReceiptHandle=receipt_handle
        )
        print('Deleted item from queue...')
        return

    text = s3.get_object(Bucket=BUCKET, Key=audio_transcribed_json)['Body']
    s3objectdata = text.read().decode()
    transcribe_json_data = json.loads(s3objectdata)
 
    print("Starting Parsing Transcription JSON and reformatting")
    csv_elements = parse_transcribe_ouput(transcribe_json_data)
    sentence = []
    speaker_tag = []
    timedur = []

    sentence = csv_elements["comment"]
    speaker_tag = csv_elements["speaker_tag"]
    timedur = csv_elements["time"]

    csv_data = io.StringIO()
    writer = csv.writer(csv_data)
    header_rec = 'TIME|speaker_tag|SENTENCE'
    writer.writerow(header_rec)
     
    rec = ' '
    i = 0
    record = []

    athena_client = boto3.client('athena')
    #Get the NamedQueryId as lambda parameter from Athena cloudformation stack output
    response = athena_client.get_named_query(
    NamedQueryId=AthenaNamedQuery
    )
    print(response['NamedQuery']['QueryString'])
    response = athena_client.start_query_execution(
    QueryString=response['NamedQuery']['QueryString'],
    ResultConfiguration={
        'OutputLocation': AthenResultsOutput
    }
    )

    # number of retries
    RETRY_COUNT = 10
    for i in range(1, 1 + RETRY_COUNT):
        # get query execution
        query_status = athena_client.get_query_execution(QueryExecutionId=response['QueryExecutionId'])
        query_execution_status = query_status['QueryExecution']['Status']['State']

        if query_execution_status == 'SUCCEEDED':
            print("Query Execution Id: " + response['QueryExecutionId'] + " ==> Attempt: " + str(i) +" ==> Query Execution Status: " + query_execution_status)
            break

        if query_execution_status == 'FAILED':
            raise Exception("Query Execution Id: " + response['QueryExecutionId'] + " ==> Attempt: " + str(i) +" ==> Query Execution Status: " + query_execution_status)

        else:
            print("Query Execution Id: " + response['QueryExecutionId'] + " ==> Attempt: " + str(i) +" ==> Query Execution Status: " + query_execution_status)
            time.sleep(i)
    else:
        athena_client.stop_query_execution(QueryExecutionId=response['QueryExecutionId'])
        raise Exception('TIME OVER')

    for item, elem in enumerate(sentence):
        #rec  = elem+"|"+speaker[item]+"|"+timedur[item]
        insert_stmt = 'insert into default.transcribe_data values('+ '\'' + audio_transcribed_job + '\'' + ',' + '\'' + timedur[item] + '\'' + ',' + '\'' + speaker_tag[item] + '\'' + ',' + '\'' + elem.replace("'","''") + '\'' + ')'
         
        response = athena_client.start_query_execution(
        QueryString=insert_stmt,
        ResultConfiguration={
             'OutputLocation': AthenResultsOutput,
             'EncryptionConfiguration': {
                 'EncryptionOption': 'SSE_S3'
             }
         }
         )
        # number of retries
        RETRY_COUNT = 10
        for i in range(1, 1 + RETRY_COUNT):
            # get query execution
            query_status = athena_client.get_query_execution(QueryExecutionId=response['QueryExecutionId'])
            query_execution_status = query_status['QueryExecution']['Status']['State']

            if query_execution_status == 'SUCCEEDED':
                 
                break

            if query_execution_status == 'FAILED':
                raise Exception("Query Execution Id: " + response['QueryExecutionId'] + " ==> Attempt: " + str(i) +" ==> Query Execution Status: " + query_execution_status)

            else:
                #print("Query Execution Id: " + response['QueryExecutionId'] + " ==> Attempt: " + str(i) +" ==> Query Execution Status: " + query_execution_status)
                time.sleep(i)
        else:
            athena_client.stop_query_execution(QueryExecutionId=response['QueryExecutionId'])
            raise Exception('TIME OVER')

        #writer.writerow(rec)
        print(rec)
    # deletes message from SQS for transcribe job after the successfully loaded athena table
    sqs.delete_message(
        QueueUrl=AudioQ,
        ReceiptHandle=receipt_handle
        )
    print('Deleted item from queue...')

    print('Publish to SNS...')
    sns = boto3.client('sns')
    response = sns.publish(
        TopicArn=os.environ['TranscribeCompletionSNSTopic'],
        Message=audio_transcribed_job
    )           
    return response