import requests
from requests.auth import HTTPBasicAuth

import uuid
import base64




# --- EDC Configuration ---
# extract the correct data offer from the received catalog
def get_data_offer(catalog_agreements):
    """
    Input:  catalog_agreements  -   List of catalog offers which we are ALLOWED to see regarding the desired asset

    Ouputs:
        error_code      - 0 (latest offer retrived) or -1 (no offers exist) 
        request_offer   - most recent offer to the asset or 'None' if no offers exist
    """
    if len(catalog_agreements) < 1:
        return -1, None
        # -> negotiate for dataset?
    
    # go over all offers to find the most recent one:
    request_offer_dates = []
    for request_offer in catalog_agreements:
        o_createdAt         = request_offer['createdAt']
        request_offer_dates.append(o_createdAt)

    # obtain the most recent offer
    idx_recent_offer = request_offer_dates.index(max(request_offer_dates))

    # get the offer of interest:
    request_offer = catalog_agreements[idx_recent_offer]
    return 0, request_offer

# - obtain the endpoint and token from the offer -
def offer2et(offer, edc_ccb, header_cpc):
    """
    Inputs:

    Outputs: 
        error_code                  - 0 (token refreshed and obtained) or -2 (http request fail)
        et_dict OR res_refreshtoken - et_dict, a dictionary with the endpoint and token
                                      res_refreshtoken, http request result with status_code, body, header, etc. 
    """
    transferProcessId = offer['transferProcessId']
    
    # request token, we always refresh here, because why not ...
    res_refreshtoken = requests.post(url=edc_ccb + '/management/v2/edrs/' + transferProcessId + '/refresh/', headers=header_cpc)
    
    if res_refreshtoken.status_code != 200: # something went wrong
        return -2, res_refreshtoken
    
    # return endpoint and token
    endpoint = res_refreshtoken.json()['endpoint']
    tx_auth  = res_refreshtoken.json()['authorization']
    et_dict  = {'endpoint': endpoint, 'token': tx_auth}

    return 0, et_dict



# --- Processing ---
def get_data_er(assetId, header_control_plane, url_edc_consumer_control_plane_base):   # get data endpoint reference (+token)
    """
    Oututs:
        status
        error info XOR et_dict

    """
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
    # query the edrs:
    res_catalog_agreement = requests.post(url=url_edc_consumer_control_plane_base + '/management/v2/edrs/request', headers=header_control_plane, json=agreement_body)
    
    res_offer, offer = get_data_offer(res_catalog_agreement.json())
    str_offer_status = "Status Offer: "+ str(res_offer)

    if res_offer != 0:
        return res_offer, str_offer_status

    # obtain the necessary data from the dictionary
    res_et, et_dict = offer2et(offer, url_edc_consumer_control_plane_base, header_control_plane)
    if res_et == -2:
        return res_et, "Token Request Failed:" + str(et_dict)
    elif res_et != 0:
        return res_et, "Unknown error"
    
    return res_et, et_dict


# --- EDR Negotiations ---
# catalog requests:
def create_generic_catalog_request_body(asset_type, edc_provider_bpn, url_edc_provider_control_plane_base):
    return {  
        "@context": {
            "@vocab": "https://w3id.org/edc/v0.0.1/ns/",
            "odrl":   "http://www.w3.org/ns/odrl/2/",
            "cx-taxo": "https://w3id.org/catenax/taxonomy#"
        },
        "@type": "CatalogRequest",
        "counterPartyId":      edc_provider_bpn,
        "counterPartyAddress": url_edc_provider_control_plane_base + "/api/v1/dsp",
        "protocol": "dataspace-protocol-http", 
        "querySpec": {
            "@type": "QuerySpec",
            "filterExpression": [
                {
                    "operandLeft": "'http://purl.org/dc/terms/type'.'@id'",
                    "operator": "=",
                    "operandRight": "https://w3id.org/catenax/taxonomy#" + asset_type,   # <- here we say what we look for!
                }
            ]
        }
    }

# 
def create_poc_ContractRequest_body(counterPartyAddress, offer_id, provider_bpn, object_of_agreement):
    return {
        "@context": [
            "https://w3id.org/tractusx/policy/v1.0.0",
            "http://www.w3.org/ns/odrl.jsonld",
            {
                "@vocab": "https://w3id.org/edc/v0.0.1/ns/"
            }
        ],
        "@type": "ContractRequest",
        "counterPartyAddress": counterPartyAddress,
        "protocol": "dataspace-protocol-http",
        "policy": {
            "@id": offer_id,
            "@type": "odrl:Offer",
            "odrl:permission": {
                "odrl:action": {
                    "odrl:type": "http://www.w3.org/ns/odrl/2/use"
                    },
                "odrl:constraint": {
                    "odrl:leftOperand": {
                        "@id": "https://factory-operator.com/terms/conditions"
                        },
                    "odrl:operator": {
                        "@id": "odrl:eq"
                        },
                    "odrl:rightOperand": "reproduce"
                }
            },
            "odrl:prohibition": [],
            "odrl:obligation": [],
            "assigner": provider_bpn,   # <- must have fields here
            "target":   object_of_agreement
        },
        "callbackAddresses": []
    }





# --- MISC Helpers ---
def str_edc_catalog(res_catalog):
    if res_catalog.status_code != 200:
        return str(res_catalog)

    str_upper_headline = "- Catalog Offer - " + res_catalog.json()['@type'] + " " + res_catalog.json()['@id']
    str_headline = "{idx:8} {id:42} {assetType:42} {idshort:42}".format(idx="Index", id="id", assetType="Action", idshort="Obligation")
    str_emph     = "-"*8 + " " + "-"*42 + " " + "-"*42 + " " + "-"*42

    str_table = str_upper_headline + "\n" + str_headline + "\n" + str_emph + "\n"

    for idx, received_dataset in enumerate(res_catalog.json()['dcat:dataset']):
        action = received_dataset['odrl:hasPolicy']['odrl:permission']['odrl:action']['odrl:type']
        obgligation = str(received_dataset['odrl:hasPolicy']['odrl:obligation'])
        str_row = "{idx:8} {id:42} {action:42} {obgligation:42}".format(idx=str(idx)+ ":", id=received_dataset["@id"][0:42],
                                                                action=action, 
                                                                obgligation=obgligation)
        str_table += str_row + "\n"

    str_table += "\n" + res_catalog.json()['dcat:service']['dcat:endpointDescription'] + " @" + res_catalog.json()['dcat:service']['dcat:endpointUrl'] + "\n"
    return str_table


# convert the response containing the registred policies to a formated string 
def str_edc_policies(res_policies):
    if res_policies.status_code != 200:
        return str(res_policies)  # string is fine here

    str_headline = "{idx:8} {id:28} {asset_id:42} {accespolicy:28} {usagepolicy:28}".format(idx="Index", id="id", asset_id="Asset", accespolicy="Access Policy", usagepolicy="Usage Policy")
    str_emph     = "-"*8 + " " + "-"*28 + " " + "-"*42 + " " + "-"*28 + " " + "-"*28

    str_table = str_headline + "\n" + str_emph + "\n"

    for idx, retrived_contract in enumerate(res_policies.json()):
        # get asset
        try:   
            asset_ = retrived_contract['assetsSelector'][0]['operandRight']
        except:
            try:  # please don't do this ...
                asset_ = retrived_contract['assetsSelector']['operandRight']
            except:
                asset_ = "None"
        str_row = "{idx:8} {id:28} {asset_id:42} {accespolicy:28} {usagepolicy:28}".format(idx=str(idx)+ ":", id=retrived_contract["@id"], 
                                                    asset_id=asset_,
                                                    accespolicy=retrived_contract['accessPolicyId'], 
                                                    usagepolicy=retrived_contract['contractPolicyId'])
        str_table += str_row + "\n"

    # wrap up the string into an html body to display it
    return  str_table


def str_edc_assets(res_get_assets):
    if res_get_assets.status_code != 200:
        return str(res_get_assets)
    
    str_headline = "{idx:8} {id:42} {assetType:28} {idshort:81}".format(idx="Index", id="id", assetType="assetType", idshort="baseUrl")
    str_emph     = "-"*8 + " " + "-"*42 + " " + "-"*28 + " " + "-"*81

    str_table = str_headline + "\n" + str_emph + "\n"

    for idx, retrived_asset in enumerate(res_get_assets.json()):
        str_row = "{idx:8} {id:42} {assetType:28} {idshort:81}".format(idx=str(idx)+ ":", id=retrived_asset["@id"][0:81],
                                                                assetType=retrived_asset['properties']['http://purl.org/dc/terms/type']['@id'].split('/')[-1].split('#')[-1], 
                                                                idshort=retrived_asset["dataAddress"]['baseUrl'])
        str_table += str_row + "\n"

    return str_table


def print_edc_assets(res_get_assets):
    print(res_get_assets)
    if res_get_assets.status_code != 200:
        return

    print("{idx:8} {id:42} {assetType:28} {idshort:81}".format(idx="Index", id="id", assetType="assetType", idshort="baseUrl"))
    print("-"*8 + " " + "-"*42 + " " + "-"*28 + " " + "-"*81)

    for idx, retrived_asset in enumerate(res_get_assets.json()):
        print("{idx:8} {id:42} {assetType:28} {idshort:81}".format(idx=str(idx)+ ":", id=retrived_asset["@id"][0:81],
                                                                assetType=retrived_asset['properties']['http://purl.org/dc/terms/type']['@id'].split('/')[-1].split('#')[-1], 
                                                                idshort=retrived_asset["dataAddress"]['baseUrl']))
   
def print_shelldescriptors(res_descriptors):
    print(res_descriptors)
    if res_descriptors.status_code != 200:
        return

    print("{idx:8} {id:42} {idshort:42}".format(idx="Index", id="globalAssetId", idshort="idShort"))
    print("-"*8 + " " + "-"*42 + " " + "-"*42)

    for idx, descriptor in enumerate(res_descriptors.json()['result']):
        if "globalAssetId" in descriptor.keys():
            print("{idx:8} {id:42} {idshort:42}".format(idx=str(idx)+ ":", id=descriptor["globalAssetId"], idshort=descriptor["idShort"]))

def print_submodels(res_get_submodels):
    print(res_get_submodels)
    if res_get_submodels.status_code != 200:
        return

    print("{idx:8} {id:81} {idshort:42}".format(idx="Index", id="id", idshort="idShort"))
    print("-"*8 + " " + "-"*81 + " " + "-"*42)

    for idx, retrived_submodel in enumerate(res_get_submodels.json()['result']):
        print("{idx:8} {id:81} {idshort:42}".format(idx=str(idx)+ ":", id=retrived_submodel["id"][0:81], idshort=retrived_submodel["idShort"]))  



# --- Templates ---
def get_submodel_template():
    return {
        "idShort": "",
        "id": "",
        "semanticId": {
            "type": "ModelReference",
            "keys": [
                {
                    "type": "Submodel",
                    "value": "https://admin-shell.io/sinksubmodel"
                }
            ]
        },
        "submodelElements": [],
        "modelType": "Submodel"
    }

def get_submodel_element_blob_template():
    return {
        "idShort": "",
        "id": "",
        "value": "",
        "semanticId": {
            "type": "ModelReference",
            "keys": [
                {
                    "type": "GlobalReference",
                    "value": "0173-1#02-AAM556#002"
                }
            ]
        },
        "contentType": "application/str",
        "modelType": "Blob"
    }