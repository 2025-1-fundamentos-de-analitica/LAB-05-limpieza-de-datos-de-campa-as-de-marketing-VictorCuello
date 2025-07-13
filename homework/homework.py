import pandas as pd
import glob
import os

def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months
    """
    input_path = "files/input/"
    output_path = "files/output/"

    os.makedirs(output_path, exist_ok=True)

    all_files = glob.glob(os.path.join(input_path, "*.csv.zip"))
    df_list = [pd.read_csv(f, compression="zip", sep=",", index_col=0) for f in all_files]
    df = pd.concat(df_list, ignore_index=True)
    
    # Limpiar nombres de columnas de posibles espacios en blanco
    df.columns = df.columns.str.strip()
    
    df["client_id"] = df.index

    # --- 1. Creación de client.csv ---
    client_df = pd.DataFrame()
    client_df["client_id"] = df["client_id"]
    client_df["age"] = df["age"]
    client_df["job"] = df["job"].str.replace(".", "", regex=False).str.replace("-", "_")
    client_df["marital"] = df["marital"]
    client_df["education"] = df["education"].str.replace(".", "_", regex=False).replace("unknown", pd.NA)
    client_df["credit_default"] = (df["credit_default"] == "yes").astype(int)
    
    # CORRECCIÓN: Usar el nombre de columna correcto: 'mortgage'
    client_df["mortgage"] = (df["mortgage"] == "yes").astype(int)

    # --- 2. Creación de campaign.csv ---
    campaign_df = pd.DataFrame()
    campaign_df["client_id"] = df["client_id"]
    campaign_df["number_contacts"] = df["number_contacts"]
    campaign_df["contact_duration"] = df["contact_duration"]
    
    # CORRECCIÓN: Usar el nombre de columna correcto: 'previous_campaign_contacts'
    campaign_df["previous_campaign_contacts"] = df["previous_campaign_contacts"]
    
    campaign_df["previous_outcome"] = (df["previous_outcome"] == "success").astype(int)
    campaign_df["campaign_outcome"] = (df["campaign_outcome"] == "yes").astype(int)
    
    month_map = {
        'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
        'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
    }
    temp_date = pd.to_datetime(
        {"year": 2022, "month": df["month"].map(month_map), "day": df["day"]}
    )
    
    campaign_df["last_contact_date"] = temp_date.dt.strftime('%Y-%m-%d')

    # --- 3. Creación de economics.csv ---
    economics_df = pd.DataFrame()
    economics_df["client_id"] = df["client_id"]
    
    economics_df["cons_price_idx"] = df["cons_price_idx"]
    economics_df["euribor_three_months"] = df["euribor_three_months"]

    # --- 4. Guardar los DataFrames en archivos CSV ---
    client_df.to_csv(os.path.join(output_path, "client.csv"), index=False)
    campaign_df.to_csv(os.path.join(output_path, "campaign.csv"), index=False)
    economics_df.to_csv(os.path.join(output_path, "economics.csv"), index=False)

    return

if __name__ == "__main__":
    clean_campaign_data()