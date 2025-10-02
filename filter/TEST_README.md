# Tests de Integración para FilterWorker (Actualizado)

Este archivo contiene tests comprehensivos de integración para todas las funciones del módulo `filter` con soporte para manejo de señales de finalización.

## Tests Implementados

### 1. `TestNewFilter` (Actualizado)
- **Objetivo**: Verificar la correcta inicialización del FilterWorker con el nuevo parámetro `workerID`
- **Casos de prueba**:
  - Creación exitosa con filtro "TbyYear" y workerID
  - Verificación de que todos los campos se inicialicen correctamente
  - **Cambios**: Ahora requiere `workerID` como primer parámetro
  - **Nota**: No se puede probar fácilmente tipos de filtro inválidos porque `NewFilter` llama a `log.Fatalf`

### 2. `TestGetFilterFunction` (Actualizado)
- **Objetivo**: Verificar que se devuelva la función de filtro correcta según el tipo (ahora es un método)
- **Casos de prueba**:
  - Tipos de filtro válidos: "TbyYear", "TbyHour", "TbyAmount", "TIbyYear"
  - Tipos de filtro inválidos
  - **Cambios**: Ahora es un método de `FilterWorker` en lugar de una función standalone

### 3. `TestFilterCallbackWithEndSignal` (Nuevo)
- **Objetivo**: Probar el comportamiento de la función callback con señales de finalización
- **Casos de prueba**:
  - Procesamiento correcto de señales de finalización
  - Verificar que no se generen errores al manejar end signals
  - **Nuevo**: Manejo de `EndSignal` y `WorkersDone`

### 4. `TestFilterWorkerIntegration` (Actualizado)
- **Objetivo**: Test de integración end-to-end que simula el flujo completo
- **Flujo de prueba**:
  1. Crea un FilterWorker con mocks de input/output y workerID
  2. Envía datos de prueba con años 2023, 2024 y 2025
  3. Verifica que solo los datos 2024/2025 pasen el filtro
  4. **Cambios**: Usa la nueva estructura `RowsBatch` con `EndSignal`

### 5. `TestEndSignalHandling` (Nuevo)
- **Objetivo**: Probar el manejo específico de señales de finalización
- **Casos de prueba**:
  - Worker único: señal de fin se envía al siguiente stage
  - Verificar que se complete el procesamiento cuando todos los workers terminan
  - **Nuevo**: Funcionalidad crítica para coordinación distribuida

## Cambios Principales en la Arquitectura

### Nuevos Campos en FilterWorker:
- `filterId`: Identificador único del worker
- `workersCount`: Número total de workers para coordinación de señales de fin

### Nueva Estructura RowsBatch:
- `EndSignal`: Booleano que indica señal de finalización
- `WorkersDone`: Array de IDs de workers que han completado
- Eliminado: `JobDone` (reemplazado por `EndSignal`)

### Nuevos Tipos de Filtro:
- `TbyYear`: Filtrado por año (reemplaza "byYear")
- `TbyHour`: Filtrado por hora
- `TbyAmount`: Filtrado por monto de transacción
- `TIbyYear`: Filtrado de items de transacción por año

### Funcionalidad de End Signal:
- Coordinación distribuida entre múltiples workers
- Propagación automática de señales de finalización
- Manejo inteligente de completitud de tareas

## Implementaciones Mock

### `MockMiddleware` (Actualizado)
Implementación mock del interface `MessageMiddleware` que permite:
- Simular envío y recepción de mensajes
- Controlar errores en operaciones
- Rastrear el estado de consumo
- Acceder a mensajes enviados para verificación
- **Nuevo**: Soporte para simulación de señales de finalización

## Notas Importantes

1. **Cambios de API**: 
   - `NewFilter` ahora requiere `workerID` como primer parámetro
   - `getFilterFunction` es ahora un método de `FilterWorker`
   - Nuevos tipos de filtro con prefijo "T"

2. **Manejo de End Signals**: Los tests verifican el correcto manejo de señales de finalización, crucial para la coordinación distribuida

3. **Limitaciones de testing**: Algunas funciones usan `log.Fatalf` que termina el proceso, lo que dificulta el testing unitario.

4. **Cobertura**: Los tests cubren:
   - Inicialización con nuevos parámetros
   - Múltiples tipos de filtro
   - Manejo de señales de finalización
   - Integración end-to-end
   - Coordinación distribuida

## Ejecución

```bash
cd filter
go test ./common -v
```

**Resultado esperado**: Todos los tests pasan exitosamente, incluyendo la nueva funcionalidad de manejo de señales de finalización.