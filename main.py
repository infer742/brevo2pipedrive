import time
import sib_api_v3_sdk
from sib_api_v3_sdk.rest import ApiException
from pprint import pprint
import pandas as pd
import streamlit as st
from dotenv import load_dotenv
import os
import json
import requests
from st_aggrid import AgGrid, GridOptionsBuilder, ColumnsAutoSizeMode
import xlsxwriter
from io import BytesIO
import numpy as np

load_dotenv()

st.set_page_config(layout="wide", page_title="Brevo Integration")

st.title("Brevo Integration for Pipedrive")


@st.cache_data(show_spinner=False)
def update_person_pipedrive(id, data, api_token, url):
    request = url + f"/api/v1/persons/{id}" + "?api_token=" + api_token
    try:
        response = requests.put(request, data)
    except:
        print("Error occurred while updating a person")


@st.cache_data
def update_persons_bulk(df, attributes, api_token, url):
    attribute_key_mappings = {field: get_person_field(
        field, api_token, url) for field in attributes}
    for index, row in df.iterrows():
        data = {map[0]: map[1][row[field]]
                for (field, map) in iter(attribute_key_mappings.items())}
        id = row["id"]
        update_person_pipedrive(id, data, api_token, url)
        time.sleep(0.1)


@st.cache_resource
def connect_to_sib(api_key):
    configuration = sib_api_v3_sdk.Configuration()
    configuration.api_key['api-key'] = api_key

    return configuration


@st.cache_data
def get_persons_pipedrive(api_token, url, page):

    request = url + \
        f"/api/v1/persons?start={page}&limit=500&api_token=" + api_token
    response = requests.get(request)
    time.sleep(2)
    json_string = response.content

    df = pd.DataFrame(json.loads(json_string)['data'])
    df["email_address"] = df.apply(
        lambda row: row["email"][0]['value'].lower(), axis=1)
    cols = ["id", "name", "first_name", "email_address"]
    if has_custom_name:
        field_key = get_person_field(
            custom_name, pipedrive_key, pipedrive_url)[0]
        df.rename(columns={field_key: custom_name}, inplace=True)
        cols.append(custom_name)
    df = df[cols]

    return df


@st.cache_data
def get_person_field(name, api_token, url):
    request = url + \
        f"/api/v1/personFields?api_token=" + api_token
    response = requests.get(request)
    json_string = response.content
    json_obj = json.loads(json_string)['data']
    field = ()
    for entry in json_obj:
        if entry['name'] == name:
            if entry['field_type'] not in ['varchar', 'int']:
                field = (entry['key'], {option['label']: option['id']
                                        for option in entry['options']})
            else:
                field = (entry['key'], {})

    return field


@st.cache_data
def convert_df(df):
    # IMPORTANT: cache_data the conversion to prevent computation on every rerun
    return df.to_csv().encode('utf-8')

@st.cache_data(persist=True)
def generate_excel_file(df, report_df):
    # keep file in memory
    output = BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    #
    
    report_df.to_excel(writer, index=False, sheet_name='report', startrow=2)

    df.to_excel(writer, index=False,sheet_name="data", startrow=1, header=False)
    
    workbook = writer.book
    worksheet = writer.sheets['report']
    data_sheet = writer.sheets['data']
    #Now we have the worksheet object. We can manipulate it 
    worksheet.set_zoom(90)
    data_sheet.set_zoom(90)
    header_format = workbook.add_format({
            "valign": "vcenter",
            "align": "center",
            "bg_color": "#951F06",
            "bold": True,
            'font_color': '#FFFFFF',
            'border' : 1, 
            'border_color': ''#D3D3D3'
        })
    #add title
    title = "Auswertung der letzten Kampagnen"
    #merge cells
    format = workbook.add_format()
    format.set_font_size(20)
    format.set_font_color("#333333")
    #
    subheader = "Kampagnen"
    worksheet.merge_range('A1:AS1', title, format)
    worksheet.merge_range('A2:AS2', subheader)
    worksheet.set_row(2, 30) 
    #worksheet.set_column(0,12,15)
    worksheet.set_column(0,0,5)
    worksheet.set_column(1,1,25)
    worksheet.set_column(2,11,15)
    worksheet.set_column(12,12,10)
    
    for i in range(len(report_df)):
        worksheet.set_row(i+3, 20)
    
    data_sheet.set_row(0, 30) 
    data_sheet.set_column(0,7,15)
    data_sheet.set_column(1,1,20)
    data_sheet.set_column(4,4,25)
    data_sheet.set_column(7,7,40)
    # puting it all together
    # Write the column headers with the defined format.
    for col_num, value in enumerate(report_df.columns.values):
        worksheet.write(2, col_num, value, header_format)
    
    
    # write data columns
    for col_num, value in enumerate(df.columns.values):
        data_sheet.write(0, col_num, value, header_format)    
    writer.close()

    # return excel file as binary string    
    return output.getvalue()


@st.cache_data()
def get_report(df):
    
    df_grouped = df.groupby(['Campaign ID', 'Campaign Name']).value_counts(subset=["Status letzte Mailkampagne"], normalize=False).reset_index()
    df_blacklist = df.groupby(['Campaign ID', 'Campaign Name'])["Blacklist"].apply(lambda x: (x == "Ja").sum()).reset_index(name='Blacklist')
    status_df = df_grouped.pivot(columns=["Status letzte Mailkampagne"], index=["Campaign ID", "Campaign Name"], values=0)
    # if some reactions did not appear, init columns manually
    reaction_cols = ["Keine Reaktion", "geöffnet", "geklickt", "Softbounce", "Hardbounce"]
    col_map = {
        'Mail geöffnet': 'geöffnet',
        'Mail Links angeklickt' : 'geklickt',
    }
    status_df = status_df.join(df_blacklist.set_index(['Campaign ID', 'Campaign Name']), on=['Campaign ID', 'Campaign Name'])

    status_df.index.set_names(["ID", "Name"], inplace=True)
    status_df = status_df.rename(columns=col_map)
    reaction_cols_full = []
    for col in reaction_cols:
        reaction_cols_full.append(col)
        reaction_cols_full.append("% \n" + col)
    status_df_cols = status_df.columns.values
    
    for col in reaction_cols_full:
        if col not in status_df_cols:
            status_df.loc[:, col] = [0.0] * len(status_df)
    
    status_df.fillna(0, inplace=True)
    status_df = status_df[reaction_cols_full + ["Blacklist"]]
    status_df["Gesamt"] = status_df.apply(lambda x: np.sum(x), axis=1)
    for col in reaction_cols:
        status_df["% \n" + col] = status_df[col] / status_df["Gesamt"]
    status_df.reset_index(inplace=True)
    
    return status_df

@st.cache_data()
def get_campaigns(_configuration, key):
    api_instance = sib_api_v3_sdk.EmailCampaignsApi(
        sib_api_v3_sdk.ApiClient(_configuration))
    type = 'classic'
    status = 'sent'
    limit = 10
    offset = 0

    try:
        api_response = api_instance.get_email_campaigns(
            type=type, status=status, limit=limit, offset=offset)
    except ApiException as e:
        print("Exception when calling EmailCampaignsApi->get_email_campaigns: %s\n" % e)

    campaigns = [{"id": campaign["id"], "name": campaign['name'],
                  "date": campaign["sentDate"]} for campaign in api_response.campaigns]

    campaigns_df = pd.DataFrame(campaigns)

    return campaigns_df


@st.cache_data
def getRecipientsCampaign(campaign_id, _configuration, key):
    email_api_instance = sib_api_v3_sdk.EmailCampaignsApi(
        sib_api_v3_sdk.ApiClient(_configuration))
    # EmailExportRecipients | Values to send for a recipient export request (optional)
    recipient_export = sib_api_v3_sdk.EmailExportRecipients(
        recipients_type="all")

    try:
        email_api_response = email_api_instance.email_export_recipients(
            campaign_id, recipient_export=recipient_export)
    except ApiException as e:
        print("Exception when calling EmailCampaignsApi->email_export_recipients: %s\n" % e)

    time.sleep(3)
    process_api_instance = sib_api_v3_sdk.ProcessApi(
        sib_api_v3_sdk.ApiClient(_configuration))
    process_id = email_api_response.process_id

    try:
        export_api_response = process_api_instance.get_process(process_id)
    except ApiException as e:
        print("Exception when calling ProcessApi->get_process: %s\n" % e)

    file_url = export_api_response.export_url
    df = pd.read_csv(file_url, sep=";")

    return df


@st.cache_data
def addStatusAndBlacklist(combined):
    combined["Status letzte Mailkampagne"] = ["Keine Reaktion"] * len(combined)
    combined["Blacklist"] = ["Nein"] * len(combined)
    combined.loc[combined["Open_Count"] > 0,
                 "Status letzte Mailkampagne"] = "Mail geöffnet"
    combined.loc[combined["Clicked_Links_Count"] > 0,
                 "Status letzte Mailkampagne"] = "Mail Links angeklickt"
    combined.loc[~pd.isna(combined["Soft_Bounce_Date"]),
                 "Status letzte Mailkampagne"] = "Softbounce"
    combined.loc[~pd.isna(combined["Hard_Bounce_Date"]),
                 "Status letzte Mailkampagne"] = "Hardbounce"
    combined.loc[~pd.isna(combined["Unsubscribe_Date"]), "Blacklist"] = "Ja"
    return combined


with st.sidebar:
    st.markdown("**Config**")
    drop_contacts = st.checkbox("Drop contacts not in pipedrive", value=False)
    drop_own_domain = st.checkbox("Drop contacts with own company domain", value= True)
    if drop_own_domain:
        if "COMPANY_DOMAIN" in os.environ:
            domain = os.environ["COMPANY_DOMAIN"]
        else:
            domain = "company.com"
        
        own_domain = st.text_input("Company domain", value=domain)

    has_custom_name = st.checkbox("Custom name field?")
    if has_custom_name:
        custom_name = st.text_input("Custom name field")


pipedrive_key = os.environ.get("PIPEDRIVE_KEY")
sib_key = os.environ.get("SIB_KEY")
pipedrive_url = os.environ.get("PIPEDRIVE_URL")

configuration = connect_to_sib(sib_key)

campaigns = get_campaigns(configuration,
                          configuration.api_key)

# TODO: integrate groups, ask users what they want
campaigns["groups"] = [i for i in range(len(campaigns))]
# builds a gridOptions dictionary using a GridOptionsBuilder instance.

campaign_groups = tuple(i for i in range(len(campaigns)))
builder = GridOptionsBuilder.from_dataframe(campaigns)
builder.configure_selection(selection_mode="multiple", use_checkbox=True)
builder.configure_column(field="groups", editable=True, groupable=True, cellEditor='agRichSelectCellEditor',
        cellEditorPopup=True,
        cellEditorParams={
            'values': campaign_groups
            })
go = builder.build()

# uses the gridOptions dictionary to configure AgGrid behavior.
st.subheader("Kampagnen")
st.info("Bitte Kampagnen auswählen")

grid = AgGrid(campaigns, gridOptions=go,
              columns_auto_size_mode=ColumnsAutoSizeMode.FIT_ALL_COLUMNS_TO_VIEW)
get_recipients = st.button("Kampagnen auswerten")

if get_recipients:
    if len(grid['selected_rows']) == 0:
        st.error("Bitte mindestens eine Kampagne auswählen!")
    else:
        campaign_ids = [row["id"] for row in grid["selected_rows"]]
        recipients = [getRecipientsCampaign(
            id, configuration, configuration.api_key) for id in campaign_ids]

        combined = pd.concat(recipients, axis=0, ignore_index=True)
        combined.sort_values(by="Campaign ID", ascending=False, inplace=True)
        combined.drop_duplicates(inplace=True, subset=[
                                 "Email_ID"], keep="first")
        combined = addStatusAndBlacklist(combined)
        contacts = [get_persons_pipedrive(
            pipedrive_key, pipedrive_url, page) for page in range(0, 1500, 500)]
        contacts = pd.concat(contacts)
        combined["Email_ID"] = combined["Email_ID"].str.lower()
        combined = combined.join(contacts.set_index(
            ["email_address"]), on="Email_ID")
        
        # drop contacts not in pipedrive
        if drop_contacts:
            combined.dropna(subset="id", inplace=True)
        
        # drop addresses from own company domain
        if drop_own_domain:
            combined = combined[~combined['Email_ID'].str.contains(f"@{own_domain}")]
            
        if has_custom_name:
            field_key = get_person_field(
                custom_name, pipedrive_key, pipedrive_url)[0]
            st.session_state['df'] = combined[['id', 'name', 'first_name', custom_name, 'Email_ID',
                                               'Status letzte Mailkampagne', 'Blacklist', 'Campaign ID', 'Campaign Name']]
        else:
            st.session_state['df'] = combined[['id', 'name', 'first_name', 'Email_ID',
                                               'Status letzte Mailkampagne', 'Blacklist', 'Campaign ID', 'Campaign Name']]

if "df" in st.session_state.keys():
    st.subheader("Ergebnis ausgewählter Kampagnen")

    edited_result_df = st.data_editor(st.session_state['df'])

    #convert to csv
    csv = convert_df(st.session_state['df'])
    # get report and export to excel
    report_df = get_report(edited_result_df)
    excel_report = generate_excel_file(edited_result_df, report_df)
    col1, col2 = st.columns(2)
    with col1:
        st.download_button(
            label="Download data as CSV",
            data=csv,
            file_name='export.csv',
            mime='text/csv',
        )
    
    with col2:
        st.download_button(
            label="Download excel report",
            data=excel_report,
            file_name='report.xlsx',
            mime="application/vnd.ms-excel",
        )
    
