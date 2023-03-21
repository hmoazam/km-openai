import os
import pickle
import numpy as np
import logging

from azure.core.credentials import AzureKeyCredential
from azure.ai.formrecognizer import DocumentAnalysisClient

from utils import storage

COG_SERV_ENDPOINT = os.environ['COG_SERV_ENDPOINT']
COG_SERV_KEY = os.environ['COG_SERV_KEY']
FR_CONTAINER = os.environ['FR_CONTAINER']
OUTPUT_BLOB_CONTAINER = os.environ['OUTPUT_BLOB_CONTAINER']


document_analysis_client = DocumentAnalysisClient(COG_SERV_ENDPOINT, AzureKeyCredential(COG_SERV_KEY))



def process_forms(in_container = FR_CONTAINER, out_container = OUTPUT_BLOB_CONTAINER): 
    blob_list = storage.list_documents(in_container)

    for b in blob_list:
        url = storage.create_sas(b, container= in_container)
        try:
            result = fr_analyze_doc(url)
            new_json = {
                'text': result,
                'doc_url': b
            }
            storage.save_json_document(new_json, container = out_container )
        except Exception as e:
            print("Error: ", str(e))

        
        


# def fr_analyze_doc(url):

#     poller = document_analysis_client.begin_analyze_document_from_url("prebuilt-document", url)
#     result = poller.result()

#     contents = ''

#     for paragraph in result.paragraphs:
#         contents += paragraph.content + '\n'

#     for kv_pair in result.key_value_pairs:
#         key = kv_pair.key.content if kv_pair.key else ''
#         value = kv_pair.value.content if kv_pair.value else ''
#         kv_pairs_str = f"{key} : {value}"
#         contents += kv_pairs_str + '\n'

#     for table_idx, table in enumerate(result.tables):
#         row = 0
#         row_str = ''
#         row_str_arr = []

#         print("*"*10)

#         print("table: ", table)
#         print("*"*10)

#         for cell in table.cells:
#             if cell.row_index == row:
#                 row_str += ' | ' + str(cell.content)
#             else:
#                 row_str_arr.append(row_str)
#                 row_str = ''
#                 row = cell.row_index
#                 row_str += ' | ' + str(cell.content)

#         row_str_arr.append(row_str)
#         contents += '\n'.join(row_str_arr) +'\n'
    
#     print("contents: ", contents)
#     print("-"*10)
#     return contents


def fr_analyze_doc(url):

    poller = document_analysis_client.begin_analyze_document_from_url("prebuilt-document", url)
    result = poller.result()

    contents = ''

    for paragraph in result.paragraphs:
        if paragraph.role == "title":
            contents += paragraph.content + '\n'

    # for kv_pair in result.key_value_pairs:
    #     key = kv_pair.key.content if kv_pair.key else ''
    #     value = kv_pair.value.content if kv_pair.value else ''
    #     kv_pairs_str = f"{key} : {value}"
    #     contents += kv_pairs_str + '\n'


    for table_idx, table in enumerate(result.tables):
        row = 0
        row_str = ''
        row_str_arr = []

        # for cell in table.cells:
        #     if cell.row_index == row:
        #         row_str += ' | ' + str(cell.content)
        #     else:
        #         row_str_arr.append(row_str)
        #         row_str = ''
        #         row = cell.row_index
        #         row_str += ' | ' + str(cell.content)
        content = []
        row_2_content = {}

        seen = set()

        sections = set(["Service description", "Service structure", "Customer category", "Channels of service provision", "Documents required", "Fees", "Steps to get the service", "Methods of service tracking", "Times of service provision", "Contact channels"])
        channels = ["Personal attendance", "Call Center 991", "Website", "Kahramaa Application"]

        for cell in table.cells:
            clean_content = cell.content.strip().strip("✓").strip()

            row = cell.row_index

            if cell.row_span == 2 and clean_content == "Channels of service provision":
                # should only be for channels of service provision
                # this is not optimal but works for now
                curr_channels = [False, False, False, False]
                for cell2 in table.cells:
                    if cell2.row_index == row+1 and cell2.row_span == 1:
                        if cell2.content not in channels and ":unselected:" not in cell2.content: # the checkboxes
                            curr_channels[cell2.column_index-1] = True
                            seen.add(cell2.row_index)
                channels_str = ""
                for i,b in enumerate(curr_channels):
                    if b:
                        channels_str += channels[i]
                        channels_str += ", "
                content.append([clean_content + ": " + channels_str])
                row_2_content[row] = len(content) - 1
                
            elif clean_content == "Contact channels":
                # contact channels  
                contact_str = ""
                num_rows = cell.row_span
                for cell2 in table.cells:
                    if cell2.row_index >= row and cell2.row_index < row+num_rows:
                        cell2_content = cell2.content.strip()
                        if (cell2_content not in sections):
                            contact_str += cell2_content 
                            contact_str += ", "
                        seen.add(cell2.row_index)
                content.append([clean_content + ": " + contact_str])
                row_2_content[row] = len(content) - 1

            else:
                if clean_content in channels:
                    continue # skip this cell as already processed
                if clean_content in sections:
                    content.append([clean_content]) # initialize so we know the row index
                    row_2_content[row] = len(content) - 1
                elif "selected" in clean_content:
                    # could be selected or unselected.
                    if ":unselected:" not in clean_content:
                        tmp_idx = clean_content.find(":selected:")
                        data = clean_content[:tmp_idx]
                        if len(data) > 0:
                            clean_data = data.strip()
                            idx = row_2_content[row]
                            content[idx].append(clean_data)
                    seen.add(row)
                else:
                    if row not in seen:
                        if row in row_2_content:
                            idx = row_2_content[row]
                            content[idx].append(clean_content)
                        else:
                            content.append([clean_content])
                            row_2_content[row] = len(content) - 1

            
        table_content = ""
        for entry in content:
            line_str = ""
            n = len(entry)
            for i in range(n):
                if i == 0:
                    line_str += entry[i] + ": "
                elif i != n-1:
                    line_str += entry[i] + ", "
                else:
                    line_str += entry[i]
            line_str += "\n"
            table_content += line_str
        

        return contents + table_content