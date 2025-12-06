# Trabajo Práctico - Sistemas Distribuidos 1 (TA050)


## Integrantes
- Patricio Ibar - 109569 - pibar@fi.uba.ar
- Valentín Marturet - 104526 - vmarturet@fi.uba.ar
- Lautaro Gastón Fritz  - 102320 - lfritz@fi.uba.ar


[Enlace al informe](https://docs.google.com/document/d/1QxOdw3MXhou0nvfM6ESwrqY2U5rpjzt_LQ8Vlrjh0rw/edit?pli=1&tab=t.0#heading=h.90zmkmxf36nb)


## Ejecución

El sistema se ejecuta a través de un `docker compose`. \
La cantidad de nodos utilizados puede ser modificada en el diccionario `nodes_count` del archivo `generar_compose.py`.\
Luego, para que el cambio sea efectivo deberá correrse dicho script.

Para ejecutar el programa recomendamos primeramente correr el script `build.sh`, para evitar que el build por defecto de `docker compose` haga imágenes extra.

Luego de buildear, el sistema puede iniciarse usando el script `start.sh` o simplemente iniciando el compose generado.

Para probar el sistema frente a fallos, puede ejecutarse el script `container_killer.sh` que por defecto simulará la falla de 3 a 5 contenedores cada 3-5 segundos. Estos parámetros pueden ser fácilmente modificados dentro del script (ver uso de los parámetros en `container_killer/main.go`).

Importante: Si se disminuye la cantidad de nodos *Monitores* del sistema, **debe** modificarse el tercer parámetro de `container_killer` para que la simulación de fallos no deje el sistema en un estado inconsistente. \
Esto se debe a que el script está configurado para ser capaz de hacer "fallar" a los monitores dejando siempre por lo menos uno, para lo cual **necesita** tener ese parámetro bien establecido.