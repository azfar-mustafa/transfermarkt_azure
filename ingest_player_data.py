import logging
import datetime
import pytz
import azure.functions as func
import azure.durable_functions as df
from datetime import datetime
import datetime
import logging
import requests
import pytz
import pandas as pd
import time
from bs4 import BeautifulSoup
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from datetime import datetime
from deltalake import write_deltalake

# To learn more about blueprints in the Python prog model V2,
# see: https://learn.microsoft.com/en-us/azure/azure-functions/functions-reference-python?tabs=asgi%2Capplication-level&pivots=python-mode-decorators#blueprints

# Note, the `func` namespace does not contain Durable Functions triggers and bindings, so to register blueprints of
# DF we need to use the `df` package's version of blueprints.
bp = df.Blueprint()

# We define a standard function-chaining DF pattern

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
    credential = DefaultAzureCredential(managed_identity_client_id='6156e8d9-e281-4380-aed1-22b1b8053c8f')

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


def get_player_attribute(player_full_page_link: str) -> str:
    """
    Get detailed players attribute

    Args:
        player_full_page_link str: Player page URL.
    
    Returns:
        str: Return full_name, height, position, foot.
    """
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}

    individual_page = requests.get(player_full_page_link, headers=headers)

    player_detail_html = BeautifulSoup(individual_page.text, 'html.parser')

    # Get player attribute data
    player_attribute = player_detail_html.find_all('span', class_=['info-table__content--regular', 'info-table__content--bold'])

    player_attribute_title = [player_attribute[i].text.strip() for i in range(len(player_attribute))]

    attribute_name = {
        'Full name': ['Full name:', 'Name in home country:'],
        'Height': ['Height:'],
        'Position': ['Position:'],
        'Foot': ['Foot:']
        }

    attribute_mapping = dict()

    for logical_name, possible_name in attribute_name.items():
        found = False
        for name in possible_name:
            if name in player_attribute_title:
                attribute_value_position = player_attribute_title.index(name,0) + 1
                if attribute_value_position < len(player_attribute_title):
                    attribute_mapping[logical_name] = player_attribute_title[attribute_value_position]
                    found = True
                    break
        if not found:
            attribute_mapping[logical_name] = 'NULL'

    full_name, height, position, foot = attribute_mapping.values()

    return full_name, height, position, foot


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


@bp.route(route="startOrchestrator/{function_name}")
@bp.durable_client_input(client_name="client")
async def start_orchestrator(req: func.HttpRequest, client):
    function_name = req.route_params.get('function_name')

    epl_season = req.params.get('epl_season')

    if epl_season:
        payload = {'epl_season': epl_season}
        instance_id = await client.start_new(function_name, None, payload)
        response = client.create_check_status_response(req, instance_id)
        logging.info(f"Started orchestration with ID = '{instance_id}'.")
    else:
        response = func.HttpResponse("Please pass a season in the query string", status_code = 400)

    return response

@bp.orchestration_trigger(context_name="context")
def my_orchestrator(context: df.DurableOrchestrationContext):
    input_context = context.get_input()
    season = input_context.get('epl_season')

    start_time = time.time()

    adls_name = "azfarsadev"
    storage_account_container = "bronze"
    azure_dev_key_vault_url = "https://azfar-keyvault-dev.vault.azure.net/"

    team_data = extract_club_link(season)
    #team_data = ['https://www.transfermarkt.com/luton-town/startseite/verein/1031/saison_id/2023', 'https://www.transfermarkt.com/sheffield-united/startseite/verein/350/saison_id/2023']

    current_date = convert_timestamp_to_myt_date()

    count_of_club_url = get_link_count(team_data)

    client_id, client_secret, client_tenant_id = get_secret_value(azure_dev_key_vault_url)

    test_parameter = {
        "team_data": team_data,
        "current_date": current_date
    }



    if count_of_club_url == 20:
        parallel_tasks = []
        for club in team_data:
            task_input = {"club_url": club, "load_date": current_date}
            parallel_tasks.append(context.call_activity('extract_player_details', task_input))
        #player_data = yield context.call_activity('extract_player_details', test_parameter)

        player_data_list = yield context.task_all(parallel_tasks)

        aggregated_player_data = [player for sublist in player_data_list for player in sublist]

        test_parameter_two = {
            "player_data": aggregated_player_data,
            "client_id": client_id,
            "client_secret": client_secret,
            "client_tenant_id": client_tenant_id,
            "storage_account_container": storage_account_container,
            "adls_name": adls_name,
            "season": season
            }

        yield context.call_activity('upload', test_parameter_two)
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    logging.info(f"Total time taken is {elapsed_time} seconds")


@bp.activity_trigger(input_name="input1")
def extract_player_details(input1: dict) -> list[str]:
    """
    Get player details for each club.

    Args:
        club_url str: String of Url club.
        load_date str: Date of data loaded.
    
    Returns:
        list[str]: List of player details contain dictionaries. Each dictionary Consists of player name, url, date of birth, country and market value.

    Reference:
        Input for parameter should be JSON serializable - https://learn.microsoft.com/en-us/azure/azure-functions/durable/durable-functions-bindings?tabs=python-v2%2Cin-process%2C2x-durable-functions&pivots=programming-language-python#trigger-sample-1
    """
    club, load_date = (input1.get(key) for key in ("club_url", "load_date"))
    logging.info(f"{club}")
    data = []

    #for club in club_url:
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}

    individual_club_url = requests.get(club, headers=headers)
    player_html = BeautifulSoup(individual_club_url.text, 'html.parser')
    players_list = player_html.find_all('tr', class_=['even', 'odd'])
    for player in players_list:
        row_data = {}
        player_name = player.find('a', href=lambda href: href and 'profil' in href) # learn lambda more
        if player_name:
            player_url_link = f"https://www.transfermarkt.com{player_name['href']}"
            player_full_name, player_height, player_detailed_position, player_preferred_foot = get_player_attribute(player_url_link)
            row_data['player_name'] = player_name.text.strip()
            row_data['player_full_name'] = player_full_name
            row_data['player_height'] = player_height
            row_data['player_detailed_position'] = player_detailed_position
            row_data['player_preferred_foot'] = player_preferred_foot
            row_data['player_link'] = f"https://www.transfermarkt.com{player_name['href']}"
            player_details = player.find_all('td', class_=['zentriert'])
            row_data['player_dob'] = player_details[1].text.split('(')[0].strip()
            row_data['player_country'] = player_details[2].find('img')['alt']
            player_value = player.find_all('td', class_=['rechts hauptlink'])
            row_data['player_value'] = player_value[0].text
            row_data['load_date'] = load_date
            data.append(row_data)
            logging.info(f"Extracted and appended data for {row_data['player_name']}.")

    return data


@bp.activity_trigger(input_name="input2")
def upload(input2: str) -> str:
    """
    Upload player data into ADLS2 as delta format

    Args:
        player_data list[str]: Azure Key Vault url
        client_id str: Service Principal client ID
        client_secret str: Service Principal client Secret
        tenant_id str: Service Principal tenant ID
        container str: Container name
        adls_name str: ADLS name
    
    Returns:
        str: Return confirmation message that it has been uploaded.
    """

    player_data, client_id, client_secret, tenant_id, container, adls_name, season = (
    input2.get(key) for key in ("player_data", "client_id", "client_secret", "client_tenant_id", "storage_account_container", "adls_name", "season")
    )

        
    storage_options = {
        'AZURE_STORAGE_ACCOUNT_NAME': adls_name,
        'AZURE_STORAGE_CLIENT_ID': client_id,
        'AZURE_STORAGE_CLIENT_SECRET': client_secret,
        'AZURE_STORAGE_TENANT_ID': tenant_id,
    }

    try:
        # Convert player list into dataframe and upload into ADLS2 as Delta format
        df = pd.DataFrame(player_data)
        write_deltalake(f"abfss://{container}@{adls_name}.dfs.core.windows.net/transfermarkt/{season}", df, mode='append', storage_options=storage_options)
        logging.info("Player data has been uploaded successfully")
        return f"Player data uploaded successfully as delta format."
    except Exception as e:
        return f"An error occurred: {str(e)}"

    