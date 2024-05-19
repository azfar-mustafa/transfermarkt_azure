import azure.functions as func
import datetime
import logging
import requests
import pytz
import pandas as pd
import tempfile
from bs4 import BeautifulSoup
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from datetime import datetime
from deltalake import write_deltalake

# Any method that is not related to func.response must be before the blueprint declaration
def extract_club_link(season: int) -> list[str]:
    """
    Extract club links from the url.

    Args:
        season (int): Specify year of the season. Example - 2023 as input is for season 2023/2024.
    
    Returns:
        list[str]: List contain string of EPL club based on the season.
    """

    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
    
    club_data = requests.get(f"https://www.transfermarkt.com/premier-league/startseite/wettbewerb/GB1/plus/?saison_id={season}", headers=headers)

    club_html = BeautifulSoup(club_data.text, 'html.parser')

    rows = club_html.find_all('tr', class_=['odd', 'even'])

    # Find all nested <td> tags with class 'hauptlink no-border-links' and extract their <a> tags with valid 'href'
    club_links = []

    for row in rows:
        tds = row.find_all('td', {'class': 'hauptlink no-border-links'})
        for td in tds:
            anchor = td.find_all('a', href=True)
            for a in anchor:
                if 'saison_id' in a['href']:
                    club_links.append('https://www.transfermarkt.com' + a['href'])

    return club_links


def get_link_count(club_url: list[str]) -> int:
    """
    Get distinct count of the url.

    Args:
        club_url list[str]: List contain string of EPL club based on the season.
    
    Returns:
        int: Count of url link. Correct count should be 20 because there is 20 clubs in the league.
    """
    unique_url = set(club_url)
    url_count = len(unique_url)
    return url_count


def extract_player_details(club_url: list[str]) -> list[str]:
    """
    Get player details for each club.

    Args:
        club_url str: String of Url club.
    
    Returns:
        list[str]: List of player details contain dictionaries. Each dictionary Consists of player name, url, date of birth, country and market value.
    """
    data = []

    for club in club_url:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
    
        individual_club_url = requests.get(club, headers=headers)

        #if club_url.status_code == 200:

        player_html = BeautifulSoup(individual_club_url.text, 'html.parser')

        players_list = player_html.find_all('tr', class_=['even', 'odd'])

        for player in players_list:
            row_data = {}
            player_name = player.find('a', href=lambda href: href and 'profil' in href) # learn lambda more
            if player_name:
                row_data['player_name'] = player_name.text.strip()
                row_data['player_link'] = player_name['href']
                player_details = player.find_all('td', class_=['zentriert'])
                row_data['player_dob'] = player_details[1].text.split('(')[0].strip()
                row_data['player_country'] = player_details[2].find('img')['alt']
                player_value = player.find_all('td', class_=['rechts hauptlink'])
                row_data['player_value'] = player_value[0].text
                data.append(row_data)

    return data


def convert_timestamp_to_myt_date() -> str:
    """
    Get date based on Kuala Lumpur timezone.
    
    Returns:
        str: Date based on Kuala Lumpur timezone.
    """
    current_utc_timestamp = datetime.utcnow()
    utc_timezone = pytz.timezone("UTC")
    myt_timezone = pytz.timezone("Asia/Kuala_Lumpur")
    myt_timestamp = utc_timezone.localize(current_utc_timestamp).astimezone(myt_timezone)
    formatted_timestamp = myt_timestamp.strftime("%d%m%Y")

    return formatted_timestamp

    

def get_secret_value(key_vault_url: str) -> str:
    """
    Get service principal details

    Args:
        key_vault_url str: Azure Key Vault url.
    
    Returns:
        str: Return client id, secret and tenant id value.
    """

    # Create a DefaultAzureCredential object to authenticate with Azure Key Vault
    credential = DefaultAzureCredential()

    # Create a SecretClient instance
    secret_client = SecretClient(vault_url=key_vault_url, credential=credential)

    # Get the secret value
    sp_secret_name = "upload-blob-adls-python"
    sp_retrieved_secret = secret_client.get_secret(sp_secret_name)

    # Get the client id
    sp_client_id = "azure-function-transfermarkt-dev-sp-client-id"
    sp_retrieved_client_id = secret_client.get_secret(sp_client_id)

    # Get the tenant id
    sp_tenant_id = "azure-function-transfermarkt-dev-sp-tenant-id"
    sp_retrieved_tenant_id = secret_client.get_secret(sp_tenant_id)

    return sp_retrieved_client_id.value, sp_retrieved_secret.value, sp_retrieved_tenant_id.value


def upload(player_list: list[str], client_id: str, client_secret: str, tenant_id: str, container: str, adls_name: str) -> str:
    """
    Upload player data into ADLS2 as delta format

    Args:
        player_list list[str]: Azure Key Vault url
        client_id str: Service Principal client ID
        client_secret str: Service Principal client Secret
        tenant_id str: Service Principal tenant ID
        container str: Container name
        adls_name str: ADLS name
    
    Returns:
        str: Return confirmation message that it has been uploaded.
    """
        
    storage_options = {
        'AZURE_STORAGE_ACCOUNT_NAME': adls_name,
        'AZURE_STORAGE_CLIENT_ID': client_id,
        'AZURE_STORAGE_CLIENT_SECRET': client_secret,
        'AZURE_STORAGE_TENANT_ID': tenant_id,
    }

    try:
        # Convert player list into dataframe and upload into ADLS2 as Delta format
        df = pd.DataFrame(player_list)
        write_deltalake(f"abfss://{container}@{adls_name}.dfs.core.windows.net/transfermarkt/tables/test", df, mode='overwrite', storage_options=storage_options)
    except Exception as e:
        return f"An error occurred: {str(e)}"

    return f"Player data uploaded successfully as delta format."
    

bp = func.Blueprint()


@bp.function_name('ingest_club_link')
@bp.route(route="newroute/{season:int}", auth_level=func.AuthLevel.ANONYMOUS)



def test_function(req: func.HttpRequest) -> func.HttpResponse:

    current_date = convert_timestamp_to_myt_date()
    adls_name = "azfarsadev"
    storage_account_container = "bronze"
    azure_dev_key_vault_url = "https://azfar-keyvault-dev.vault.azure.net/"
    
    season = req.route_params.get('season')

    #team_data = extract_club_link(season)
    team_data = ['https://www.transfermarkt.com/luton-town/startseite/verein/1031/saison_id/2023', 'https://www.transfermarkt.com/sheffield-united/startseite/verein/350/saison_id/2023']
    logging.info('EPL club link has been extracted')

    count_of_club_url = get_link_count(team_data)

    local_temppath = tempfile.gettempdir()
    file_name = f"transfermarkt_{current_date}.parquet"
    local_filepath = f"{local_temppath}/{file_name}"
    blob_filepath = f"player_transfermarkt/current/{current_date}/{file_name}"

    client_id, client_secret, client_tenant_id = get_secret_value(azure_dev_key_vault_url)

    if count_of_club_url == 2:
        player_data = extract_player_details(team_data)
        upload(player_data, client_id, client_secret, client_tenant_id, storage_account_container, adls_name)

        #for club in data:
        #    local_temppath = tempfile.gettempdir()
        #    club_name = "luton"
        #    local_filepath = f"{local_temppath}/{club_name}_{current_date}.parquet"
        #    player_json = extract_player_details(club)
            #convert_list_into_dataframe(player_json, local_filepath)
            #upload_file_to_blob(local_filepath, current_date, storage_account_url, storage_account_container)
        
        return func.HttpResponse(
            f"Data has been uploaded successfully",
            status_code=200
        )
    else:
        error_message = f"Error: Expected 20 unique URLs, but found {count_of_club_url}."
        logging.error(error_message)
        return func.HttpResponse(
            body=error_message,
            status_code=400,
            mimetype="text/plain"
        )