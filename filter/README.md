# Filter Service

## Tipos de filtro

- **TbyYear**: Filtra transacciones que no se hayan hecho en los años 2024 y 2025
- **TbyHour**: Filtra transacciones que no se hayan hecho entre las 6 y las 23 horas
- **TbyAmount**: Filtra transacciones cuyo monto sea menor a 75
- **TIbyYear**: Filtra Los items de las transacciones que no se hayan hecho en los años 2024 y 2025

## Variables de entorno requeridas

```bash
FILTER_ID=1                     # ID único del worker
WORKERS_COUNT=4                 # Cantidad de workers con los que se levanto este servicio.
FILTER_TYPE=TbyYear             # Tipo de filtro a aplicar
CONSUMER_NAME=filter-consumer   # Nombre del consumer de RabbitMQ
MW_ADDRESS=amqp://guest:guest@localhost:5672/  # Dirección del middleware
SOURCE_QUEUE=input-queue        # Cola de entrada
OUTPUT_EXCHANGE=output-exchange # Exchange de salida
```

## Ejecución

```bash
# Compilar
go build

# Ejecutar con variables de entorno
export FILTER_ID="1"
export FILTER_TYPE="TbyYear"
# ... (configurar todas las variables)
./filter
```

## Graceful Shutdown

El servicio maneja señales del sistema para realizar un apagado ordenado:

- **SIGTERM**: Apagado ordenado (usado por Docker, Kubernetes, etc.)
- **SIGINT**: Interrupción por teclado (Ctrl+C)

### Secuencia de apagado:

1. Recibe señal SIGTERM/SIGINT
2. Detiene el procesamiento de nuevos mensajes
3. Completa el procesamiento de mensajes en curso
4. Cierra conexiones del middleware
5. Termina el programa

### Timeout:

- **30 segundos** máximo para apagado graceful
- Si no se completa en ese tiempo, fuerza la terminación

