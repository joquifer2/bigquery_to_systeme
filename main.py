import requests
import json
from google.cloud import bigquery
from functions_framework import http

@http
def export_to_systeme(request):
    # Define la URL y los encabezados para la solicitud API
    url = "https://api.systeme.io/api/contacts"
    headers = {
        "accept": "application/json",
        "X-API-Key": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
    }

    # Obtener el ID de la etiqueta "Formulario nativo meta"
    tag_name = "Formulario nativo meta"
    tags_url = "https://api.systeme.io/api/tags"
    tags_response = requests.get(tags_url, headers=headers)

    # Mostrar la respuesta para analizar la estructura
    print("Respuesta de la API de etiquetas:")
    print(tags_response.text)

    # Intenta convertir la respuesta a JSON para procesarla
    try:
        tags_data = tags_response.json()
    except json.JSONDecodeError as e:
        print(f"Error al decodificar JSON: {e}")
        return 'Error al obtener etiquetas de Systeme.io', 500

    tag_id = None

    # Ahora accedemos a la lista de etiquetas dentro de 'items'
    if 'items' in tags_data:
        for tag in tags_data['items']:  # Iterar sobre la lista de etiquetas
            if tag.get('name') == tag_name:
                tag_id = tag.get('id')
                print(f"ID de la etiqueta '{tag_name}' encontrado: {tag_id}")
                break
    else:
        print(f"Formato inesperado de la respuesta: {tags_data}")
        return 'Formato inesperado de la respuesta de etiquetas', 500

    if not tag_id:
        print(f"Error: no se pudo encontrar la etiqueta '{tag_name}' en Systeme.io.")
        return 'Etiqueta no encontrada', 500

    # Conectar a BigQuery con el ID del proyecto
    client = bigquery.Client(project='compact-record-429209-h9')  # Reemplaza con tu ID de proyecto de Google Cloud
    query = """
        SELECT full_name, email, country, leadgen_id FROM `compact-record-429209-h9.formularios.t_forms_meta`
        WHERE email IS NOT NULL AND sent_to_systeme = FALSE
    """
    query_job = client.query(query)

    # Procesar los resultados
    for row in query_job:
        try:
            # Extraer el primer nombre del campo full_name
            first_name = row["full_name"].split(" ")[0]  # Tomar solo el primer nombre
            country = row["country"]  # Asegurarse de obtener el campo 'country'
            leadgen_id = row["leadgen_id"]  # Obtener el leadgen_id

            data = {
                "email": row["email"],
                "fields": [
                    {
                        "slug": "first_name",
                        "value": first_name
                    },
                    {
                        "slug": "country",
                        "value": country
                    },
                    {
                        "slug": "leadgen_id",
                        "value": leadgen_id
                    }
                ]
            }

            # Crear contacto en Systeme.io
            response = requests.post(url, headers=headers, json=data)
            response.raise_for_status()  # Lanza una excepción para códigos de estado 4xx o 5xx

            if response.status_code == 201:  # Verificar que se ha creado correctamente
                contact_data = response.json()
                contact_id = contact_data.get("id")  # Obtener el ID del contacto creado
                print(f"Contacto {row['email']} añadido correctamente a Systeme.io con ID {contact_id}")

                # Asignar la etiqueta usando el tagId
                tag_url = f"https://api.systeme.io/api/contacts/{contact_id}/tags"
                tag_data = {
                    "tagId": tag_id
                }
                tag_response = requests.post(tag_url, headers=headers, json=tag_data)

                # Manejar tanto 200 como 204 como éxito
                if tag_response.status_code in [200, 204]:
                    print(f"Etiqueta 'Formulario nativo meta' asignada al contacto {row['email']}.")
                else:
                    print(f"Error al asignar la etiqueta: {tag_response.status_code} - {tag_response.text}")

                # Marcar como enviado en BigQuery
                update_query = f"""
                    UPDATE `compact-record-429209-h9.formularios.t_forms_meta`
                    SET sent_to_systeme = TRUE
                    WHERE email = '{row["email"]}'
                """
                try:
                    client.query(update_query)
                    print(f"Contacto {row['email']} marcado como enviado en BigQuery.")
                except Exception as e:
                    print(f"Error al actualizar BigQuery para {row['email']}: {e}")

            elif response.status_code == 422 and "Este valor ya se ha utilizado." in response.text:
                print(f"Contacto {row['email']} ya existe en Systeme.io, no se crea un duplicado.")

        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred for {row['email']}: {http_err}")
        except requests.exceptions.ConnectionError as conn_err:
            print(f"Connection error occurred for {row['email']}: {conn_err}")
        except requests.exceptions.Timeout as timeout_err:
            print(f"Timeout error occurred for {row['email']}: {timeout_err}")
        except requests.exceptions.RequestException as req_err:
            print(f"An error occurred for {row['email']}: {req_err}")
        except KeyError as key_err:
            print(f"Missing data for {row['email']}: {key_err}")
        except Exception as e:
            print(f"An unexpected error occurred for {row['email']}: {e}")

    return 'Integración completada', 200
