import boto3

def insult_filter_batch(_):
    insults = {"fool", "idiot", "dumb", "moron"}

    sqs = boto3.client('sqs', region_name='us-east-1')
    queue_url = 'https://sqs.us-east-1.amazonaws.com/164573654608/InsultTextQueue'

    resultados = []

    while True:
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=2
        )

        if 'Messages' not in response:
            break  # no más mensajes

        for msg in response['Messages']:
            texto = msg['Body']
            palabras = texto.split()
            filtrado = " ".join("CENSORED" if w in insults else w for w in palabras)
            resultados.append(f"Original: {texto} → {filtrado}")

            # Eliminar de la cola
            sqs.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=msg['ReceiptHandle']
            )

    return resultados
