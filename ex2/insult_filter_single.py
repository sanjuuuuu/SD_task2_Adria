import boto3

def insult_filter_single(_):
    insults = {"fool", "idiot", "dumb", "moron"}

    sqs = boto3.client('sqs', region_name='us-east-1')
    queue_url = 'https://sqs.us-east-1.amazonaws.com/164573654608/InsultTextQueue'

    response = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=1
    )

    if 'Messages' not in response:
        return "No message received"

    msg = response['Messages'][0]
    texto = msg['Body']
    palabras = texto.split()
    filtrado = " ".join("CENSORED" if w.lower() in insults else w for w in palabras)

    sqs.delete_message(
        QueueUrl=queue_url,
        ReceiptHandle=msg['ReceiptHandle']
    )

    return f"Original: {texto} → {filtrado}"
