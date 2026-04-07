from dotenv import load_dotenv
import os
import logging
import json
import time
import datetime
import requests
from requests.auth import HTTPBasicAuth
from azure.storage.blob import BlobServiceClient

load_dotenv()

# ── Eloqua Bulk API ──────────────────────────────────────────────────────────
BULK_BASE_URL = "https://secure.p04.eloqua.com/api/bulk/2.0/"
REST_BASE_URL = "https://secure.p04.eloqua.com/api/REST/2.0/"
ELOQUA_USERNAME = os.getenv("ELOQUA_USERNAME")
ELOQUA_PASSWORD = os.getenv("ELOQUA_PASSWORD")
ELOQUA_AUTH = HTTPBasicAuth(ELOQUA_USERNAME, ELOQUA_PASSWORD)

PAGE_SIZE = 50000

# ── ADLS / Blob Storage ─────────────────────────────────────────────────────
ADLS_ACCOUNT_NAME = os.getenv("ADLS_ACCOUNT_NAME")
ADLS_ACCOUNT_KEY = os.getenv("ADLS_ACCOUNT_KEY")
ADLS_CONTAINER = os.getenv("ADLS_CONTAINER", "eloqua")

# ── Databricks job trigger ───────────────────────────────────────────────────
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_JOB_ID = os.getenv("DATABRICKS_ELOQUA_JOB_ID")

# ── Logging ──────────────────────────────────────────────────────────────────
logging.getLogger("azure").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

log = logging.getLogger("eloqua")
log.setLevel(logging.INFO)
_fh = logging.FileHandler(os.path.join(os.path.dirname(os.path.abspath(__file__)), "ingest_errors.log"))
_fh.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
log.addHandler(_fh)

# ── Contact export field definition ──────────────────────────────────────────
CONTACT_EXPORT_DEF = {
    "name": "Contact Export",
    "fields": {
        "EmailAddress": "{{Contact.Field(C_EmailAddress)}}",
        "FirstName": "{{Contact.Field(C_FirstName)}}",
        "LastName": "{{Contact.Field(C_LastName)}}",
        "Company": "{{Contact.Field(C_Company)}}",
        "EmailDisplayName": "{{Contact.Field(C_EmailDisplayName)}}",
        "Address1": "{{Contact.Field(C_Address1)}}",
        "Address2": "{{Contact.Field(C_Address2)}}",
        "Address3": "{{Contact.Field(C_Address3)}}",
        "City": "{{Contact.Field(C_City)}}",
        "State_Prov": "{{Contact.Field(C_State_Prov)}}",
        "Zip_Postal": "{{Contact.Field(C_Zip_Postal)}}",
        "Country": "{{Contact.Field(C_Country)}}",
        "BusPhone": "{{Contact.Field(C_BusPhone)}}",
        "MobilePhone": "{{Contact.Field(C_MobilePhone)}}",
        "Fax": "{{Contact.Field(C_Fax)}}",
        "Title": "{{Contact.Field(C_Title)}}",
        "Salutation": "{{Contact.Field(C_Salutation)}}",
        "Salesperson": "{{Contact.Field(C_Salesperson)}}",
        "SFDCContactID": "{{Contact.Field(C_SFDCContactID)}}",
        "SFDCLeadID": "{{Contact.Field(C_SFDCLeadID)}}",
        "DateCreated": "{{Contact.Field(C_DateCreated)}}",
        "DateModified": "{{Contact.Field(C_DateModified)}}",
        "ContactIDExt": "{{Contact.Field(ContactIDExt)}}",
        "SFDCAccountID": "{{Contact.Field(C_SFDCAccountID)}}",
        "LastModifiedByExtIntegrateSystem": "{{Contact.Field(C_LastModifiedByExtIntegrateSystem)}}",
        "SFDCLastCampaignID": "{{Contact.Field(C_SFDCLastCampaignID)}}",
        "SFDCLastCampaignStatus": "{{Contact.Field(C_SFDCLastCampaignStatus)}}",
        "Company_Revenue1": "{{Contact.Field(C_Company_Revenue1)}}",
        "SFDEmailOptOut1": "{{Contact.Field(C_SFDC_EmailOptOut1)}}",
        "Lead_Source___Most_Recent1": "{{Contact.Field(C_Lead_Source___Most_Recent1)}}",
        "Lead_Source___Original1": "{{Contact.Field(C_Lead_Source___Original1)}}",
        "Industry1": "{{Contact.Field(C_Industry1)}}",
        "Annual_Revenue1": "{{Contact.Field(C_Annual_Revenue1)}}",
        "Lead_Status1": "{{Contact.Field(C_Lead_Status1)}}",
        "Job_Role1": "{{Contact.Field(C_Job_Role1)}}",
        "LS___High_Value_Website_Content1": "{{Contact.Field(C_LS___High_Value_Website_Content1)}}",
        "Lead_Score_Date___Most_Recent1": "{{Contact.Field(C_Lead_Score_Date___Most_Recent1)}}",
        "Integrated_Marketing_and_Sales_Funnel_Stage": "{{Contact.Field(C_Integrated_Marketing_and_Sales_Funnel_Stage)}}",
        "Product_Solution_of_Interest1": "{{Contact.Field(C_Product_Solution_of_Interest1)}}",
        "Region1": "{{Contact.Field(C_Region1)}}",
        "elqPURLName1": "{{Contact.Field(C_elqPURLName1)}}",
        "Lead_Rating___Combined1": "{{Contact.Field(C_Lead_Rating___Combined1)}}",
        "EmailAddressDomain": "{{Contact.Field(C_EmailAddressDomain)}}",
        "FirstAndLastName": "{{Contact.Field(C_FirstAndLastName)}}",
        "Company_Size1": "{{Contact.Field(C_Company_Size1)}}",
        "Lead_Score___Last_High_Touch_Event_Date1": "{{Contact.Field(C_Lead_Score___Last_High_Touch_Event_Date1)}}",
        "Lead_Rating___Explicit1": "{{Contact.Field(C_Lead_Rating___Explicit1)}}",
        "Lead_Rating___Implicit1": "{{Contact.Field(C_Lead_Rating___Implicit1)}}",
        "Lead_Score___Explicit1": "{{Contact.Field(C_Lead_Score___Explicit1)}}",
        "Lead_Score___Implicit1": "{{Contact.Field(C_Lead_Score___Implicit1)}}",
        "Lead_Score_Date___Profile___Most_Recent1": "{{Contact.Field(C_Lead_Score_Date___Profile___Most_Recent1)}}",
        "Employees1": "{{Contact.Field(C_Employees1)}}",
        "Territory": "{{Contact.Field(C_Territory)}}",
        "ElqPURLName": "{{Contact.Field(C_ElqPURLName)}}",
        "Middle_Name1": "{{Contact.Field(C_Middle_Name1)}}",
        "Account_ID1": "{{Contact.Field(C_Account_ID1)}}",
        "Customer_Name_ID1": "{{Contact.Field(C_Customer_Name_ID1)}}",
        "Account_Type_Description1": "{{Contact.Field(C_Account_Type_Description1)}}",
        "Daytime_Phone1": "{{Contact.Field(C_Daytime_Phone1)}}",
        "Evening_Phone1": "{{Contact.Field(C_Evening_Phone1)}}",
        "Gender1": "{{Contact.Field(C_Gender1)}}",
        "Since_Date1": "{{Contact.Field(C_Since_Date1)}}",
        "Solicit_Daytime_Phone1": "{{Contact.Field(C_Solicit_Daytime_Phone1)}}",
        "Solicit_Evening_Phone1": "{{Contact.Field(C_Solicit_Evening_Phone1)}}",
        "Solicit_Cell_Phone1": "{{Contact.Field(C_Solicit_Cell_Phone1)}}",
        "Solicit_Address1": "{{Contact.Field(C_Solicit_Address1)}}",
        "Solicit_Email1": "{{Contact.Field(C_Solicit_Email1)}}",
        "Status_Name1": "{{Contact.Field(C_Status_Name1)}}",
        "Birth_Month1": "{{Contact.Field(C_Birth_Month1)}}",
        "Birth_Year1": "{{Contact.Field(C_Birth_Year1)}}",
        "Season_Seat_Location_Preference1": "{{Contact.Field(C_Season_Seat_Location_Preference1)}}",
        "Group_Ticket___Number_of_Guests1": "{{Contact.Field(C_Group_Ticket___Number_of_Guests1)}}",
        "Group_Section_Preference1": "{{Contact.Field(C_Group_Section_Preference1)}}",
        "Suite_Package1": "{{Contact.Field(C_Suite_Package1)}}",
        "Team_News_Subscription1": "{{Contact.Field(C_Team_News_Subscription1)}}",
        "Ticket_Offers_Subscription1": "{{Contact.Field(C_Ticket_Offers_Subscription1)}}",
        "Special_Offers_Subscription1": "{{Contact.Field(C_Special_Offers_Subscription1)}}",
        "Epsilon_Created_Date1": "{{Contact.Field(C_Epsilon_Created_Date1)}}",
        "Cust_Support_Rep1": "{{Contact.Field(C_Cust_Support_Rep1)}}",
        "Primary_Contact1": "{{Contact.Field(C_Primary_Contact1)}}",
        "Primary_Email1": "{{Contact.Field(C_Primary_Email1)}}",
        "Interested_in_Redskins_Gold_1": "{{Contact.Field(C_Interested_in_Redskins_Gold_1)}}",
        "Lead_Source_Description1": "{{Contact.Field(C_Lead_Source_Description1)}}",
        "Military_Status1": "{{Contact.Field(C_Military_Status1)}}",
        "Military_Branch1": "{{Contact.Field(C_Military_Branch1)}}",
        "Military_Base1": "{{Contact.Field(C_Military_Base1)}}",
        "Lead_Source_Description___Original1": "{{Contact.Field(C_Lead_Source_Description___Original1)}}",
        "Game_Pass1": "{{Contact.Field(C_Game_Pass1)}}",
        "Fanatics1": "{{Contact.Field(C_Fanatics1)}}",
        "NFL_Pro_Shop1": "{{Contact.Field(C_NFL_Pro_Shop1)}}",
        "Favorite_Current_Player1": "{{Contact.Field(C_Favorite_Current_Player1)}}",
        "Favorite_All_Time_Player1": "{{Contact.Field(C_Favorite_All_Time_Player1)}}",
        "Renewal_Window___Date1": "{{Contact.Field(C_Renewal_Window___Date1)}}",
        "Renewal_Window___Time1": "{{Contact.Field(C_Renewal_Window___Time1)}}",
        "Renewal_Total_Balance1": "{{Contact.Field(C_Renewal_Total_Balance1)}}",
        "Renewal_Credit_Balance1": "{{Contact.Field(C_Renewal_Credit_Balance1)}}",
        "Renewal_After_Credit_Balance1": "{{Contact.Field(C_Renewal_After_Credit_Balance1)}}",
        "Renewal_Monthly_Payment1": "{{Contact.Field(C_Renewal_Monthly_Payment1)}}",
        "Ticket_Sub1": "{{Contact.Field(C_Ticket_Sub1)}}",
        "Team_Updates_Sub1": "{{Contact.Field(C_Team_Updates_Sub1)}}",
        "Price_Per_Seat1": "{{Contact.Field(C_Price_Per_Seat1)}}",
        "Account_Executive1": "{{Contact.Field(C_Account_Executive1)}}",
        "Webbula_Classification1": "{{Contact.Field(C_Webbula_Classification1)}}",
        "DSalute_Subscription1": "{{Contact.Field(C_DC_Salute_Subscription1)}}",
        "FB___Full_Name1": "{{Contact.Field(C_FB___Full_Name1)}}",
        "DWomen_of_Washington_Subscription1": "{{Contact.Field(C_DC_Women_of_Washington_Subscription1)}}",
        "DTicket_Offers_FXF_Events_Subscription1": "{{Contact.Field(C_DC_Ticket_Offers_FXF_Events_Subscription1)}}",
        "DWashington_Rally_Subscription1": "{{Contact.Field(C_DC_Washington_Rally_Subscription1)}}",
        "DCharitable_Foundation_Subscription1": "{{Contact.Field(C_DC_Charitable_Foundation_Subscription1)}}",
        "DCommunity_Programming___Events_Subscript": "{{Contact.Field(C_DC_Community_Programming___Events_Subscript)}}",
        "DYouth_Programming___Events_Subscription1": "{{Contact.Field(C_DC_Youth_Programming___Events_Subscription1)}}",
        "DGold1": "{{Contact.Field(C_DC_Gold1)}}",
        "Survey_Link1": "{{Contact.Field(C_Survey_Link1)}}",
        "Temp_Sean_Taylor_Rally_Towel_Tracking_Numbe": "{{Contact.Field(C_Temp_Sean_Taylor_Rally_Towel_Tracking_Numbe)}}",
        "Cust_Support_Rep_Phone1": "{{Contact.Field(C_Cust_Support_Rep_Phone1)}}",
        "Cust_Support_Rep_Email1": "{{Contact.Field(C_Cust_Support_Rep_Email1)}}",
        "Unsubscribe_Link1": "{{Contact.Field(C_Unsubscribe_Link1)}}",
        "Id": "{{Contact.Id}}",
        "IsSubscribed": "{{Contact.Email.IsSubscribed}}",
        "IsBounced": "{{Contact.Email.IsBounced}}",
        "EmailFormat": "{{Contact.Email.Format}}",
    },
    "maxRecords": 5000000,
}


# ── ADLS helpers ─────────────────────────────────────────────────────────────
def get_blob_client():
    return BlobServiceClient(
        account_url=f"https://{ADLS_ACCOUNT_NAME}.blob.core.windows.net",
        credential=ADLS_ACCOUNT_KEY,
    )


def _blob_upload(blob_client, path, data: bytes):
    container = blob_client.get_container_client(ADLS_CONTAINER)
    container.get_blob_client(path).upload_blob(data, overwrite=True)


def write_batch(blob_client, table, records, batch_num=0):
    timestamp = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
    path = f"bronze/{table}/pending/{timestamp}_{batch_num:04d}.json"
    ndjson = "\n".join(json.dumps(r) for r in records)
    _blob_upload(blob_client, path, ndjson.encode("utf-8"))


# ── Eloqua Bulk API helpers ──────────────────────────────────────────────────
def _bulk_post(endpoint, data=None, json_body=None):
    for attempt in range(3):
        try:
            resp = requests.post(
                BULK_BASE_URL + endpoint,
                headers={"content-type": "application/json"},
                data=data,
                json=json_body,
                auth=ELOQUA_AUTH,
                timeout=30,
            )
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            log.error(f"[bulk POST {endpoint}] attempt {attempt + 1}/3: {e}")
            if attempt < 2:
                time.sleep(2 ** attempt)
    raise RuntimeError(f"Eloqua bulk POST {endpoint} failed after 3 attempts")


def _bulk_get(endpoint, params=None):
    for attempt in range(3):
        try:
            resp = requests.get(
                BULK_BASE_URL + endpoint,
                headers={"content-type": "application/json"},
                params=params,
                auth=ELOQUA_AUTH,
                timeout=60,
            )
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            log.error(f"[bulk GET {endpoint}] attempt {attempt + 1}/3: {e}")
            if attempt < 2:
                time.sleep(2 ** attempt)
    raise RuntimeError(f"Eloqua bulk GET {endpoint} failed after 3 attempts")


def _rest_delete(endpoint):
    try:
        requests.delete(
            REST_BASE_URL + endpoint,
            headers={"content-type": "application/json"},
            auth=ELOQUA_AUTH,
            timeout=30,
        )
        print("Export definition deleted")
    except requests.RequestException as e:
        log.error(f"[delete {endpoint}] {e}")


# ── Eloqua export workflow ───────────────────────────────────────────────────
def create_export():
    response = _bulk_post("contacts/exports", json_body=CONTACT_EXPORT_DEF)
    uri = response["uri"]
    print(f"Export definition created: {uri}")
    return uri


def sync_export(uri):
    body = {"syncedInstanceUri": uri}
    response = _bulk_post("syncs", json_body=body)
    sync_uri = response["uri"]
    print(f"Sync started: {sync_uri}")
    return sync_uri


def poll_sync(sync_uri, max_wait=600):
    elapsed = 0
    delay = 2
    while elapsed < max_wait:
        time.sleep(delay)
        elapsed += delay
        data = _bulk_get(sync_uri)
        status = data.get("status", "")
        print(f"  Sync status: {status} ({elapsed}s elapsed)")

        if status == "success":
            return True
        if status in ("error", "warning"):
            log.error(f"Sync failed: {json.dumps(data)}")
            raise RuntimeError(f"Sync {sync_uri} ended with status: {status}")

        delay = min(delay * 2, 30)

    raise RuntimeError(f"Sync {sync_uri} timed out after {max_wait}s")


def fetch_and_upload(blob_client, sync_uri):
    offset = 0
    batch = 0
    total = 0

    while True:
        data = _bulk_get(f"{sync_uri}/data", params={"limit": PAGE_SIZE, "offset": offset})

        if "totalResults" not in data:
            log.error(f"Unexpected response at offset {offset}: {json.dumps(data)[:500]}")
            break

        total_results = data["totalResults"]
        items = data.get("items", [])
        has_more = data.get("hasMore", False)

        if not items:
            break

        write_batch(blob_client, "eloqua_contacts", items, batch_num=batch)
        batch += 1
        total += len(items)
        print(f"  Batch {batch}: {len(items)} records (total {total} of {total_results})")

        if not has_more:
            break

        offset += PAGE_SIZE

    return total


# ── Databricks job trigger ───────────────────────────────────────────────────
def trigger_databricks_job():
    if not all([DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_JOB_ID]):
        print("Databricks job trigger not configured — skipping")
        return

    resp = requests.post(
        f"{DATABRICKS_HOST}/api/2.1/jobs/run-now",
        headers={"Authorization": f"Bearer {DATABRICKS_TOKEN}"},
        json={"job_id": int(DATABRICKS_JOB_ID)},
    )
    if resp.status_code == 200:
        run_id = resp.json().get("run_id")
        print(f"Databricks job triggered — run_id: {run_id}")
    else:
        log.error(f"failed to trigger Databricks job: {resp.text}")
        print(f"WARNING: Databricks job trigger failed — {resp.text}")


# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    blob_client = get_blob_client()
    start = time.time()

    # Step 1: Create export definition
    uri = create_export()

    try:
        # Step 2: Start sync
        sync_uri = sync_export(uri)

        # Step 3: Poll until data is ready
        poll_sync(sync_uri)

        # Step 4: Fetch pages and write to ADLS
        total = fetch_and_upload(blob_client, sync_uri)

    finally:
        # Step 5: Always clean up the export definition
        _rest_delete(uri)

    elapsed = time.time() - start
    print(f"\nEloqua contacts ingest complete in {elapsed:.1f}s")
    print(f"  contacts: {total} records")

    trigger_databricks_job()


if __name__ == "__main__":
    main()
