from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse
import requests

import uuid     # in python
import base64   # in python
import copy     # in python
import yaml     # MIT

from enum import Enum   # in python
from typing import Annotated

# in order to see what's happening under the hood
import logging

from .utils import create_generic_catalog_request_body, get_data_offer, offer2et, create_poc_ContractRequest_body
from .utils import get_data_er
from .utils import str_edc_catalog

from .mbcs_business_logic import csvblob2dataframe, evaluate_usecase

# create application w/logging
app = FastAPI()
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s', 
                    handlers=[
                        logging.FileHandler('example.log'),
                    ])#filename='example.log')

# obtain config:
with open('consumer_cfg.yaml', 'r') as file:
    consumer_cfg = yaml.safe_load(file)

# - control plane -
url_edc_consumer_control_plane_base = consumer_cfg['consumer-edc-control-plane']['endpoint']
header_control_plane = consumer_cfg['consumer-edc-control-plane']['header'] # this contains secrets, so please use -at least- a secretsmanager instead

# - "identities" -
edc_provider_bpn = consumer_cfg['trusted-providers']['Facotry_Operator_A']['BPN']  # "{{EDCTX-10-1-BPN}}"
url_edc_provider_control_plane_base = consumer_cfg['trusted-providers']['Facotry_Operator_A']['endpoint-control-plane']


class allowed_asset_types(str, Enum):
    Submodel = "Submodel"
    DigitalTwinRegistry = "DigitalTwinRegistry" 



# --- General ---
@app.get("/")
async def root():
    return "Machine Builder Condition Service [PoC TP2.04 & TP4.1]"

@app.get("/config")     # REMOVE THIS BECAUSE YOU CAN ACCESS SECRETS!!!
async def get_condig():
    return consumer_cfg


# obtain/view available offers
@app.get("/provider-offers") 
async def get_offers(provider_bpn: Annotated[str, Query(min_length=3, max_length=28)] = edc_provider_bpn,
                     asset_type: Annotated[allowed_asset_types, Query()] = "Submodel"):
    
    # - discovery -
    # say what we are looking for:
    catalog_request_body = create_generic_catalog_request_body(asset_type, provider_bpn, url_edc_provider_control_plane_base)
    # note: the provider edc control plane should also be a parameter

    # query edc and format repsone into a nice table:
    res_catalog = requests.post(url=url_edc_consumer_control_plane_base + '/management/v2/catalog/request', headers=header_control_plane, json=catalog_request_body)
    str_catalog_table = str_edc_catalog(res_catalog)  

    # wrap up the string into an html body to display it
    return  HTMLResponse(content=f"""<html><body><pre>{str_catalog_table}</pre></body></html>""")


# view endpoint + token
@app.get("/edrendpoint")    # example: /edrendpoint?assetId=simple_test    
async def get_et_dict(assetId: Annotated[str, Query(min_length=3, max_length=42)]): 
    res_et, et_dict = get_data_er(assetId, header_control_plane, url_edc_consumer_control_plane_base)
    # NOTE: the header and control-plane-endpoint should be parameters aswell
    #       here we simplified for the PoC
    return {"Status": res_et, "Endpoint-Info": et_dict}


# CX-0127
@app.get('/FO/connect-to-parent')   # example: /FO/connect-to-parent?assetId=simple_test  
async def push_asset(assetId: Annotated[str, Query(min_length=3, max_length=42)]):
    res_et, et_dict = get_data_er(assetId, header_control_plane, url_edc_consumer_control_plane_base)
    if res_et != 0:
        # start renegotiation
        return res_et, et_dict

    # if successful this far, get data:
    # meta data:
    res_data_info = requests.get(url=et_dict['endpoint'], headers={'Authorization': et_dict['token']})
    if res_data_info.status_code != 200:
        return -10, str(res_data_info)
    # actual data
    res_data = requests.get(url=et_dict['endpoint'] + '/$value?extent=WithBlobValue', headers={'Authorization': et_dict['token']})
    if res_data.status_code != 200:
        return -11, str(res_data)
    

    # --- processing ---
    element_id = res_data_info.json()['submodelElements'][0]['idShort']
    value_enc  = res_data.json()[element_id]['value']


    if res_data.json()[element_id]['contentType'] == 'application/str':
        value = base64.b64decode(value_enc).decode('utf-8')
        return value
    if res_data.json()[element_id]['contentType'] == 'application/csv':
        df_gen = csvblob2dataframe(value_enc)

        # process: 
        mb_analysis_result = evaluate_usecase(df_gen)
        return mb_analysis_result
    return "Unknown Value"


# check if there's an offer for the requested asset:
@app.get("/edrs")
async def get_offers(assetId: Annotated[str, Query(min_length=3, max_length=42)]): 
    # see if there are some edrs which have beennegotiated for
    agreement_body = {
        "@context": {
            "@vocab": "https://w3id.org/edc/v0.0.1/ns/"
        },
        "@type": "QuerySpec",
        "filterExpression": [
            {
                "operandLeft": "assetId",
                "operator": "=",
                "operandRight": assetId,
            }
        ]
    }

    res_catalog_agreement = requests.post(url=url_edc_consumer_control_plane_base + '/management/v2/edrs/request', headers=header_control_plane, json=agreement_body)
    
    res_offer, offer = get_data_offer(res_catalog_agreement.json())
    str_offer_status = "Status Offer: "+ str(res_offer)

    if res_offer != 0:
        return str_offer_status

    # obtain the necessary data from the dictionary
    res_et, et_dict = offer2et(offer, url_edc_consumer_control_plane_base, header_control_plane)
    if res_et == -2:
        return "Token Request Failed:" + str(et_dict)
    elif res_et != 0:
        return "Unknown error"
    else:
        return offer

    

def negotiate_for_offer(object_of_agreement, asset_type, provider_bpn):
    # request the catalog with the offer:
    catalog_request_body = create_generic_catalog_request_body(asset_type, provider_bpn, url_edc_provider_control_plane_base)
    res_catalog = requests.post(url=url_edc_consumer_control_plane_base + '/management/v2/catalog/request', headers=header_control_plane, json=catalog_request_body)
    if res_catalog.status_code != 200:
        return -2, res_catalog     # request fail
    
    dct_endpointUrl, offer_id = None, None
    for dcat_dataset in res_catalog.json()['dcat:dataset']:
        # look for the dataset with our id:
        if dcat_dataset['@id'] == object_of_agreement:
            asset_policy = dcat_dataset['odrl:hasPolicy']
            offer_id     = asset_policy['@id']

            # get negotiation endpoint: dct_endpointUrl
            dct_endpointUrl = None
            for distribution_method in dcat_dataset['dcat:distribution']:
                if distribution_method['dct:format']['@id'] == 'HttpData-PULL':
                    dct_endpointUrl = distribution_method['dcat:accessService']['dct:endpointUrl']
                    break
            # check if we actuall got the desired endpoint        
            if dct_endpointUrl is not None:
                break
    
    if dct_endpointUrl is None:
        return -1, None     # offer fail

    edr_negotiation_body = create_poc_ContractRequest_body(dct_endpointUrl, offer_id, provider_bpn, object_of_agreement)
    res_edr_negotiation  = requests.post(url=url_edc_consumer_control_plane_base + '/management/v2/edrs', headers=header_control_plane, json=edr_negotiation_body)
    if res_edr_negotiation.status_code != 200:
        return -3, res_edr_negotiation     # request fail
    
    # get negotiation id:
    edr_negotation_id = res_edr_negotiation.json()['@id']
    return 0, {'Negotioation-ID:', edr_negotation_id}
