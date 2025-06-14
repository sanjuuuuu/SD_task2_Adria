import lithops
import boto3

BUCKET_NAME = 'exercise3-bucket-sanjuu' 
INSULTOS = {'fool', 'idiot', 'moron', 'dumb'}

def map_censor_line(archivo, index, line):
    palabras = line.strip().split()
    censuradas = [w if w.lower() not in INSULTOS else 'CENSORED' for w in palabras]
    censurada_linea = ' '.join(censuradas)

    # Guardar línea censurada en archivo temporal local
    output_path = f"/tmp/{archivo}_censored.txt"
    with open(output_path, 'a') as f:
        f.write(censurada_linea + '\n')

    num_insultos = sum(1 for w in palabras if w.lower() in INSULTOS)
    return archivo, num_insultos


def reduce_sum(results):
    # Agrupar resultados por archivo y contar total de insultos
    totales = {}
    for archivo, num in results:
        totales[archivo] = totales.get(archivo, 0) + num

    # Subir archivos censurados a S3
    s3 = boto3.client('s3')
    for archivo in totales:
        censored_path = f"/tmp/{archivo}_censored.txt"
        s3.upload_file(censored_path, BUCKET_NAME, f"{archivo}_censored.txt")

    return totales

def main():
    # Cliente S3 para listar archivos
    s3 = boto3.client('s3')
    objects = s3.list_objects_v2(Bucket=BUCKET_NAME)
    txt_files = [
        obj['Key'] for obj in objects.get('Contents', [])
        if obj['Key'].endswith('.txt') and not obj['Key'].endswith('_censored.txt')
    ]

    if not txt_files:
        print("⚠️ No se encontraron archivos .txt no censurados en el bucket.")
        return

    # Descargar archivos y preparar líneas para map
    line_data = []
    for archivo in txt_files:
        response = s3.get_object(Bucket=BUCKET_NAME, Key=archivo)
        contenido = response['Body'].read().decode('utf-8')
        lineas = contenido.splitlines()
        line_data.extend([(archivo, i, l) for i, l in enumerate(lineas)])

    # Lanzar procesamiento con Lithops
    fexec = lithops.FunctionExecutor()
    future = fexec.map_reduce(map_censor_line, line_data, reduce_sum)
    resultados = future.get_result()

    for archivo, total in resultados.items():
        print(f"✅ {archivo}: {total} insultos censurados")

if __name__ == "__main__":
    main()
