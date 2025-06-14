import lithops
import boto3
from time import sleep

BUCKET_NAME = 'exercise3-bucket-sanjuu'
INSULTOS = {'fool', 'idiot', 'moron', 'dumb'}

# ---------------------------
# 🔧 Función que procesa 1 archivo entero
# ---------------------------
def censor_file(filename):
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=BUCKET_NAME, Key=filename)
    contenido = response['Body'].read().decode('utf-8')
    
    censuradas = []
    contador = 0

    for line in contenido.splitlines():
        palabras = line.strip().split()
        censurada_linea = [w if w.lower() not in INSULTOS else 'CENSORED' for w in palabras]
        censuradas.append(' '.join(censurada_linea))
        contador += sum(1 for w in palabras if w.lower() in INSULTOS)

    # Guardar resultado censurado en local
    censored_path = f"/tmp/{filename}_censored.txt"
    with open(censored_path, 'w') as f:
        f.write('\n'.join(censuradas))

    # Subir a S3 sobrescribiendo versión anterior
    s3.upload_file(censored_path, BUCKET_NAME, f"{filename}_censored.txt")

    return filename, contador

# ---------------------------
# 🚀 Función batch que lanza maxfunc archivos por vez
# ---------------------------
def batch(function, maxfunc, bucket):
    s3 = boto3.client('s3')
    objects = s3.list_objects_v2(Bucket=bucket)
    
    # Filtrar archivos válidos
    txt_files = [
        obj['Key'] for obj in objects.get('Contents', [])
        if obj['Key'].endswith('.txt') and not obj['Key'].endswith('_censored.txt')
    ]

    if not txt_files:
        print("⚠️ No hay archivos .txt sin censurar en el bucket.")
        return

    # Dividir en chunks de maxfunc
    def chunks(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    # Ejecutar por lotes
    resultados_totales = {}
    for grupo in chunks(txt_files, maxfunc):
        fexec = lithops.FunctionExecutor()
        futures = fexec.map(function, grupo)
        resultados = fexec.get_result()
        for archivo, censurados in resultados:
            resultados_totales[archivo] = censurados

    return resultados_totales

# ---------------------------
# 🧪 Main para testear Exercise 4
# ---------------------------
def main():
    max_concurrent = 2  # Cambiar esto para probar batches más pequeños
    resultados = batch(censor_file, max_concurrent, BUCKET_NAME)

    for archivo, total in resultados.items():
        print(f"✅ {archivo}: {total} insultos censurados")

if __name__ == "__main__":
    main()
