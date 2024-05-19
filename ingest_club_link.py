import azure.functions as func
import datetime
import json
import logging
import requests
import time
import pytz
import pandas as pd
import tempfile
import io
from bs4 import BeautifulSoup
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from datetime import datetime
from deltalake import DeltaTable, write_deltalake

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


def convert_timestamp_to_myt_date():
    current_utc_timestamp = datetime.utcnow()
    utc_timezone = pytz.timezone("UTC")
    myt_timezone = pytz.timezone("Asia/Kuala_Lumpur")
    myt_timestamp = utc_timezone.localize(current_utc_timestamp).astimezone(myt_timezone)
    formatted_timestamp = myt_timestamp.strftime("%d%m%Y")
    return formatted_timestamp

    

def get_secret_value():
    KEY_VAULT_URL = "https://azfar-keyvault-dev.vault.azure.net/"

    # Create a DefaultAzureCredential object to authenticate with Azure Key Vault
    credential = DefaultAzureCredential()

    # Create a SecretClient instance
    secret_client = SecretClient(vault_url=KEY_VAULT_URL, credential=credential)

    # Get the secret value
    sp_secret_name = "upload-blob-adls-python"
    sp_retrieved_secret = secret_client.get_secret(sp_secret_name)

    sp_client_id = "azure-function-transfermarkt-dev-sp-client-id"
    sp_retrieved_client_id = secret_client.get_secret(sp_client_id)

    sp_tenant_id = "azure-function-transfermarkt-dev-sp-tenant-id"
    sp_retrieved_tenant_id = secret_client.get_secret(sp_tenant_id)

    # Print the secret value
    return sp_retrieved_client_id.value, sp_retrieved_secret.value, sp_retrieved_tenant_id.value


def upload(data, client_id, client_secret, tenant_id):
    storage_options = {
        'AZURE_STORAGE_ACCOUNT_NAME': 'azfarsadev',
        'AZURE_STORAGE_CLIENT_ID': client_id,
        'AZURE_STORAGE_CLIENT_SECRET': client_secret,
        'AZURE_STORAGE_TENANT_ID': tenant_id,
    }

    
    # write some data into a delta table
    df = pd.DataFrame(data)
    write_deltalake("abfss://bronze@azfarsadev.dfs.core.windows.net/transfermarkt/tables/test", df, mode='overwrite', storage_options=storage_options)
    

bp = func.Blueprint()


@bp.function_name('ingest_club_link')
@bp.route(route="newroute/{season:int}", auth_level=func.AuthLevel.ANONYMOUS)



def test_function(req: func.HttpRequest) -> func.HttpResponse:

    current_date = convert_timestamp_to_myt_date()
    storage_account_url = "https://azfarsadev.blob.core.windows.net"
    storage_account_container = "bronze"
    

    season = req.route_params.get('season')

    #team_data = extract_club_link(season)
    team_data = ['https://www.transfermarkt.com/luton-town/startseite/verein/1031/saison_id/2023', 'https://www.transfermarkt.com/sheffield-united/startseite/verein/350/saison_id/2023']
    #player_data = [{'player_name': 'Thomas Kaminski', 'player_link': '/thomas-kaminski/profil/spieler/77757', 'player_dob': 'Oct 23, 1992', 'player_country': 'Belgium', 'player_value': '€2.80m'}, {'player_name': 'Tim Krul', 'player_link': '/tim-krul/profil/spieler/33027', 'player_dob': 'Apr 3, 1988', 'player_country': 'Netherlands', 'player_value': '€500k'}, {'player_name': 'Jack Walton', 'player_link': '/jack-walton/profil/spieler/368629', 'player_dob': 'Apr 23, 1998', 'player_country': 'England', 'player_value': '€400k'}, {'player_name': 'James Shea', 'player_link': '/james-shea/profil/spieler/91340', 'player_dob': 'Jun 16, 1991', 'player_country': 'England', 'player_value': '€200k'}, {'player_name': 'Teden Mengi', 'player_link': '/teden-mengi/profil/spieler/548470', 'player_dob': 'Apr 30, 2002', 'player_country': 'England', 'player_value': '€12.00m'}, {'player_name': 'Tom Lockyer', 'player_link': '/tom-lockyer/profil/spieler/207742', 'player_dob': 'Dec 3, 1994', 'player_country': 'Wales', 'player_value': '€3.50m'}, {'player_name': 'Gabriel Osho', 'player_link': '/gabriel-osho/profil/spieler/364409', 'player_dob': 'Aug 14, 1998', 'player_country': 'Nigeria', 'player_value': '€3.00m'}, {'player_name': 'Mads Andersen', 'player_link': '/mads-andersen/profil/spieler/407021', 'player_dob': 'Dec 27, 1997', 'player_country': 'Denmark', 'player_value': '€3.00m'}, {'player_name': 'Reece Burke', 'player_link': '/reece-burke/profil/spieler/264220', 'player_dob': 'Sep 2, 1996', 'player_country': 'England', 'player_value': '€1.80m'}, {'player_name': "Amari'i Bell", 'player_link': '/amarii-bell/profil/spieler/278166', 'player_dob': 'May 5, 1994', 'player_country': 'Jamaica', 'player_value': '€2.50m'}, {'player_name': 'Dan Potts', 'player_link': '/dan-potts/profil/spieler/207037', 'player_dob': 'Apr 13, 1994', 'player_country': 'England', 'player_value': '€900k'}, {'player_name': 'Issa Kaboré', 'player_link': '/issa-kabore/profil/spieler/649452', 'player_dob': 'May 12, 2001', 'player_country': 'Burkina Faso', 'player_value': '€10.00m'}, {'player_name': 'Daiki Hashioka', 'player_link': '/daiki-hashioka/profil/spieler/387191', 'player_dob': 'May 17, 1999', 'player_country': 'Japan', 'player_value': '€2.00m'}, {'player_name': 'Marvelous Nakamba', 'player_link': '/marvelous-nakamba/profil/spieler/324882', 'player_dob': 'Jan 19, 1994', 'player_country': 'Zimbabwe', 'player_value': '€5.00m'}, {'player_name': 'Pelly Ruddock Mpanzu', 'player_link': '/pelly-ruddock-mpanzu/profil/spieler/244338', 'player_dob': 'Mar 22, 1994', 'player_country': 'DR Congo', 'player_value': '€1.50m'}, {'player_name': 'Albert Sambi Lokonga', 'player_link': '/albert-sambi-lokonga/profil/spieler/381967', 'player_dob': 'Oct 22, 1999', 'player_country': 'Belgium', 'player_value': '€15.00m'}, {'player_name': 'Ross Barkley', 'player_link': '/ross-barkley/profil/spieler/131978', 'player_dob': 'Dec 5, 1993', 'player_country': 'England', 'player_value': '€8.00m'}, {'player_name': 'Jordan Clark', 'player_link': '/jordan-clark/profil/spieler/184129', 'player_dob': 'Sep 22, 1993', 'player_country': 'England', 'player_value': '€1.00m'}, {'player_name': 'Luke Berry', 'player_link': '/luke-berry/profil/spieler/125685', 'player_dob': 'Jul 12, 1992', 'player_country': 'England', 'player_value': '€400k'}, {'player_name': 'Elliot Thorpe', 'player_link': '/elliot-thorpe/profil/spieler/496661', 'player_dob': 'Nov 9, 2000', 'player_country': 'Wales', 'player_value': '€200k'}, {'player_name': 'Alfie Doughty', 'player_link': '/alfie-doughty/profil/spieler/608175', 'player_dob': 'Dec 21, 1999', 'player_country': 'England', 'player_value': '€7.00m'}, {'player_name': 'Fred Onyedinma', 'player_link': '/fred-onyedinma/profil/spieler/305274', 'player_dob': 'Nov 24, 1996', 'player_country': 'Nigeria', 'player_value': '€500k'}, {'player_name': 'Chiedozie Ogbene', 'player_link': '/chiedozie-ogbene/profil/spieler/392591', 'player_dob': 'May 1, 1997', 'player_country': 'Ireland', 'player_value': '€8.00m'}, {'player_name': 'Tahith Chong', 'player_link': '/tahith-chong/profil/spieler/344830', 'player_dob': 'Dec 4, 1999', 'player_country': 'Netherlands', 'player_value': '€4.50m'}, {'player_name': 'Andros Townsend', 'player_link': '/andros-townsend/profil/spieler/61842', 'player_dob': 'Jul 16, 1991', 'player_country': 'England', 'player_value': '€1.80m'}, {'player_name': 'Carlton Morris', 'player_link': '/carlton-morris/profil/spieler/246963', 'player_dob': 'Dec 16, 1995', 'player_country': 'England', 'player_value': '€13.00m'}, {'player_name': 'Elijah Adebayo', 'player_link': '/elijah-adebayo/profil/spieler/319900', 'player_dob': 'Jan 7, 1998', 'player_country': 'England', 'player_value': '€12.00m'}, {'player_name': 'Jacob Brown', 'player_link': '/jacob-brown/profil/spieler/469958', 'player_dob': 'Apr 10, 1998', 'player_country': 'Scotland', 'player_value': '€4.00m'}, {'player_name': 'Cauley Woodrow', 'player_link': '/cauley-woodrow/profil/spieler/169801', 'player_dob': 'Dec 2, 1994', 'player_country': 'England', 'player_value': '€1.00m'}]
    logging.info('EPL club link has been extracted')

    count_of_club_url = get_link_count(team_data)

    local_temppath = tempfile.gettempdir()
    file_name = f"transfermarkt_{current_date}.parquet"
    local_filepath = f"{local_temppath}/{file_name}"
    blob_filepath = f"player_transfermarkt/current/{current_date}/{file_name}"

    client_id, client_secret, client_tenant_id = get_secret_value()

    if count_of_club_url == 2:
        player_data = extract_player_details(team_data)
        upload(player_data, client_id, client_secret, client_tenant_id)

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