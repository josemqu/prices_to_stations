# Precios a Estaciones: CSV to JSON con Geocodificación

Este proyecto convierte un archivo CSV con listados históricos de precios de combustibles en Argentina a un formato JSON estructurado por estación y producto. Además, corrige y completa las coordenadas geográficas (latitud y longitud) de las estaciones usando la API de Google Maps.

## Requisitos

- Python 3.8+
- Instalar dependencias:
  ```bash
  pip install -r requirements.txt
  ```
- Archivo `.env` en el directorio del script con tu API Key de Google Maps:
  ```env
  API_KEY=tu_clave_de_google
  ```

## Archivos de entrada y salida

- **Entrada:** `precios-historicos.csv`
- **Salida:**
  - `stations_prices.json`: JSON estructurado con estaciones, productos y precios.
  - `precios-historicos-updated.csv`: CSV con coordenadas corregidas (si hubo cambios).

## Uso

1. **Configura tu entorno:**
   - Asegúrate de tener Python y las dependencias instaladas.
   - Coloca el archivo `.env` con tu API Key en el mismo directorio que el script.
   - Coloca el archivo `precios-historicos.csv` en el mismo directorio.

2. **Ejecuta el script:**
   ```bash
   python csv_to_json.py
   ```

   El script:
   - Lee el CSV de precios históricos.
   - Agrupa los datos por estación y producto.
   - Si encuentra estaciones con coordenadas faltantes o inválidas, consulta la API de Google Maps para completarlas (de manera asíncrona y eficiente).
   - Genera el archivo `stations_prices.json` listo para usar.

3. **Resultado:**
   - El progreso se muestra en pantalla (incluyendo geocodificación).
   - El archivo JSON estará en el mismo directorio.

## Estructura del JSON de salida

```json
[
  {
    "stationId": 123,
    "stationName": "NOMBRE ESTACIÓN",
    "address": "DIRECCIÓN",
    "town": "LOCALIDAD",
    "province": "PROVINCIA",
    "flag": "BANDERA",
    "flagId": 1,
    "geometry": {
      "type": "Point",
      "coordinates": [LONGITUD, LATITUD]
    },
    "products": [
      {
        "productId": 1,
        "productName": "PRODUCTO",
        "prices": [
          {
            "price": 100.0,
            "date": "2024-01-01T12:00:00Z",
            "hourType": "Diurno",
            "hourTypeId": 2
          }
        ]
      }
    ]
  }
]
```

## Notas
- Si tienes errores de certificados SSL en Mac, ejecuta:
  ```bash
  /Applications/Python\ 3.13/Install\ Certificates.command
  ```
- El script es eficiente y usa concurrencia para geocodificación, pero respeta los límites de la API de Google.
- Si no tienes API Key, el script funcionará pero no corregirá coordenadas.

## Licencia
MIT
