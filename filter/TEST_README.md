# Tests de Integración para FilterWorker

Este archivo contiene tests comprehensivos de integración para todas las funciones del módulo `filter`.

## Tests Implementados

### 1. `TestParseTimestamp`
- **Objetivo**: Verificar el parsing correcto de timestamps en formato "2006-01-02 15:04:05"
- **Casos de prueba**:
  - Timestamps válidos con diferentes fechas
  - Formatos inválidos (separadores incorrectos, faltan datos, cadena vacía)
  - Formatos de tiempo incorrectos

### 2. `TestFilterRowsByYear`
- **Objetivo**: Probar la función de filtrado por año con asignación de semestres
- **Casos de prueba**:
  - Datos válidos de 2024 primer semestre (enero-junio)
  - Datos válidos de 2025 segundo semestre (julio-diciembre)
  - Mezcla de años (solo 2024/2025 deben pasar)
  - Errores: columna "year" no encontrada, filas con columnas insuficientes
  - Formato de timestamp inválido
  - Columna year que no es string
  - Batch vacío (debe funcionar correctamente)

### 3. `TestGetFilterFunction`
- **Objetivo**: Verificar que se devuelva la función de filtro correcta según el tipo
- **Casos de prueba**:
  - Tipo de filtro válido ("byYear")
  - Tipos de filtro inválidos
  - Tipo de filtro vacío

### 4. `TestFilterCallbackFunction`
- **Objetivo**: Probar el comportamiento de la función callback del filtro
- **Casos de prueba**:
  - Batch válido con datos de 2024
  - Batch vacío (no debe enviar datos)
  - JSON inválido (debe generar error)

### 5. `TestNewFilter`
- **Objetivo**: Verificar la correcta inicialización del FilterWorker
- **Casos de prueba**:
  - Creación exitosa con filtro "byYear"
  - Verificación de que todos los campos se inicialicen correctamente
  - **Nota**: No se puede probar fácilmente tipos de filtro inválidos porque `NewFilter` llama a `log.Fatalf`

### 6. `TestFilterWorkerIntegration`
- **Objetivo**: Test de integración end-to-end que simula el flujo completo
- **Flujo de prueba**:
  1. Crea un FilterWorker con mocks de input/output
  2. Envía datos de prueba con años 2023, 2024 y 2025
  3. Verifica que solo los datos 2024/2025 pasen el filtro
  4. Confirma que se agregue correctamente la columna "semester"
  5. Valida la asignación correcta de semestres (primer/segundo semestre)

### 7. `TestFilterWorkerStartAndClose`
- **Objetivo**: Probar los métodos Start y Close del FilterWorker
- **Casos de prueba**:
  - Inicio y cierre exitoso del worker
  - Verificación de que el middleware comience y detenga el consumo correctamente

### 8. `TestFilterWorkerErrorHandling`
- **Objetivo**: Probar manejo de errores en el FilterWorker
- **Casos de prueba**:
  - Error al enviar mensajes (output middleware falla)
  - JSON inválido en el callback (debe manejarse graciosamente)

## Implementaciones Mock

### `MockMiddleware`
Implementación mock del interface `MessageMiddleware` que permite:
- Simular envío y recepción de mensajes
- Controlar errores en operaciones
- Rastrear el estado de consumo
- Acceder a mensajes enviados para verificación

## Notas Importantes

1. **Limitaciones de testing**: Algunas funciones usan `log.Fatalf` que termina el proceso, lo que dificulta el testing unitario. En un entorno de producción se recomienda refactorizar para devolver errores en lugar de terminar el proceso.

2. **Cobertura**: Los tests cubren tanto casos exitosos como escenarios de error, incluyendo:
   - Validación de datos de entrada
   - Manejo de errores de parsing
   - Comportamiento con datos vacíos
   - Filtrado correcto por años y semestres
   - Flujo de comunicación completo

3. **Concurrencia**: Los tests incluyen manejo de goroutines y channels para simular el comportamiento real del sistema distribuido.

## Ejecución

```bash
cd filter
go test ./common -v
```

Todos los tests deben pasar exitosamente, proporcionando confianza en el correcto funcionamiento del módulo de filtrado.