#Cloud Function
import functions_framework
import pandas as pd
import hashlib
import re
import string
import datetime
import gcsfs
import openpyxl
import sqlalchemy
from google.cloud.sql.connector import Connector
from email.message import EmailMessage
import smtplib
from google.cloud import secretmanager
import pathlib
import numpy as np

from signal import signal, SIGPIPE, SIG_DFL  
signal(SIGPIPE,SIG_DFL)

bucket_name = "devrb-eumr-cs-rnd-edcam-poc1"

def send_mail(e, file_type):
  
    sender = "rbone.management@rb.com"
    recipient = ["rajat.kashyap@reckitt.com", "tuntun.kumar@reckitt.com"] 
    message = f"""Hi All,<br>
                   We have encountered an error while running the RnD pipeline.<br>
                   Please find the following details :<br>
                   <p><strong>Component : Cloud Function<br>
                   Source File : {file_type}<br>
                   Error : {e}</strong></p>
                   Regards,<br>RnD Pipeline"""
    
    email = EmailMessage()
    email["From"] = sender
    email["To"] = recipient
    email["Subject"] = "Error encountered in RnD pipeline"
    email.set_content(message)
    email.add_alternative(message, subtype="html")

    smtp = smtplib.SMTP("smtp.office365.com", port=587)
    smtp.starttls()
    smtp.login(sender, "dO,36Fql)d-34SG!?14")
    smtp.sendmail(sender, recipient, email.as_string())
    smtp.quit()

def getSecret(version_id='latest'):
    PROJECT_ID = "dev-consumer-data-hub"
    secret_id = "RnD-secret"
    # Build the resource name of the secret version.
    name = f"projects/{PROJECT_ID}/secrets/{secret_id}/versions/{version_id}"
    client = secretmanager.SecretManagerServiceClient()
    # Access the secret version.
    response = client.access_secret_version(name=name)

    # Return the decoded payload.
    return str(response.payload.data.decode('UTF-8'))
    
def getconn():
    connector = Connector()
    conn = connector.connect(
         "dev-consumer-data-hub:europe-west2:devrb-euw2-psql-rnd-edcam-poc",
         "pg8000",
         user="edcam",
         db="postgres",
         password=getSecret("latest"),
         enable_iam_auth=True,
     )
    return conn

def remove_from_database(file,sheet,form):
    conn = getconn()
    cur = conn.cursor()
    if form == "hut":
        query1 = f"delete from hut_data_cf where file_name = '{file}' and sheet_name = '{sheet}'"
        query2 = f"delete from hut_metadata_cf where file_name = '{file}' and sheet_name = '{sheet}'"
        cur.execute(query1)
        cur.execute(query2)
        conn.commit()
        #conn.close()
    elif form == "clt":
        query1 = f"delete from clt_data_cf where file_name = '{file}' and sheet_name = '{sheet}'"
        query2 = f"delete from clt_metadata_cf where file_name = '{file}' and sheet_name = '{sheet}'"
        cur.execute(query1)
        cur.execute(query2)
        conn.commit()
        #conn.close()
    else:
        query1 = f"delete from sensory_data_cf where file_name = '{file}'"
        query2 = f"delete from sensory_metadata_cf where file_name = '{file}'"
        cur.execute(query1)
        cur.execute(query2)
        conn.commit()
        #conn.close()
    return

def define_sensory_metadata_table():
    column_metadata = ["Device_settings",
    "Device_name",
    "Experiment_batch",
    "Experiment_dataType",
    "Experiment_dateLastModified",
    "Experiment_equipment",
    "Experiment_ID",
    "Experiment_laboratory",
    "Experiment_objective",
    "Experiment_onBehalf",
    "Experiment_owner",
    "Experiment_projectName",
    "Experiment_riskAssessmentCode",
    "Experiment_seriesNumber",
    "Experiment_setDescription",
    "Experiment_setID",
    "Experiment_SOPNumber",
    "Experiment_startDate",
    "Experiment_title",
    "Fragrance_ID",
    "Product_ID",
    "Product_description",
    "Product_format",
    "File_name",
    "Sheet_name"]
    return pd.DataFrame(columns = column_metadata)

def define_sensory_data_table():
    column_data = ["Measurement_significance",
    "Measurement_timestamp",
    "Measurement_repetition",
    "Measurement_product",
    "Time_point",
    "Measurement_mean",
    "Experiment_ID",
    "Experiment_batch",
    "File_name",
    "Sheet_name"]
    
    return pd.DataFrame(columns = column_data)

def guess_row_col(df, instring):
    row, column = (df.applymap(lambda x: x if re.search(instring,str(x)) else None )).values.nonzero()
    t = list(zip(row,column))
    return t

def get_data(df,coord):
    try:
        value = str(df.iloc[coord[0][0],coord[0][1]+1])
        return value
    except:
        pass
    
    return "NA"

def change_to_date(x):
    x = re.sub('Sept', 'Sep', x)
    x = re.sub('Apt', 'Apr', x)
    try:
        return datetime.datetime.strptime(x, '%d/%b/%Y').strftime('%Y-%m-%d')
    except:
        pass
    
    try:
        return datetime.datetime.strptime(x, '%d/%m/%Y').strftime('%Y-%m-%d')
    except:
        pass
    
    if x == 'NA':
        return x
    return re.sub(r'00:00:00','',x)

def sensory_ingestion(data, data_meta):
    string_list = ["measurement_significance",
        "measurement_product",
        "experiment_id",
        "experiment_batch",
        "file_name",
        "sheet_name",
        "time_point",
        "measurement_repetition"]
    date_list = ["measurement_timestamp"]

    string_list_meta = ["device_settings",
        "device_name",
        "experiment_batch",
        "experiment_datatype",
        "experiment_equipment",
        "experiment_id",
        "experiment_laboratory",
        "experiment_objective",
        "experiment_onbehalf",
        "experiment_owner",
        "experiment_projectname",
        "experiment_riskassessmentcode",
        "experiment_setdescription",
        "experiment_setid",
        "experiment_sopnumber",
        "experiment_title",
        "fragrance_id",
        "product_id",
        "product_description",
        "product_format",
        "file_name",
        "sheet_name"]

    date_list_meta = ["experiment_datelastmodified", "experiment_startdate"]
    int_list_meta = ["experiment_seriesnumber"]
    
    data.columns = [x.lower() for x in data.columns]
    data_meta.columns = [x.lower() for x in data_meta.columns]
    
    for x in string_list:
        data[x].fillna("NA", inplace = True)
    for x in date_list:
        data[x].fillna(pd.Timestamp("19000101"), inplace=True) 
        data[x]= pd.to_datetime(data[x])
    for x in string_list_meta:
        data_meta[x].fillna("NA", inplace = True)
    for x in date_list_meta:
        data_meta[x].fillna(pd.Timestamp("19000101"), inplace=True) 
        data_meta[x]= pd.to_datetime(data_meta[x])
    for x in int_list_meta:
        data_meta[x].fillna(0,inplace = True)

    
    data['measurement_mean'] = data['measurement_mean'].replace([np.nan, ''], 0)
    data['measurement_mean'] = data['measurement_mean'].astype(float)
    
    db = sqlalchemy.create_engine(
        "postgresql+pg8000://",
        creator=getconn,
        pool_pre_ping=True
    )
    
    print("DATA INGESTION PROCESS STARTED....")
    data.to_sql('sensory_data_cf', con = db, index=False, if_exists = 'append', chunksize = 1000, method='multi')
    data_meta.to_sql('sensory_metadata_cf', con = db, index=False, if_exists = 'append', chunksize = 1000, method='multi')
    print("DATA INGESTION PROCESS FINISHED!!")
    #db.close()
    return

def sensory_parser():
    final_metadata = define_sensory_metadata_table()
    final_data = define_sensory_data_table()

    config1 = pd.read_csv("gs://{0}/cf_test_input/sensoryconfig/config1.csv".format(bucket_name),dtype=str)
    config2 = pd.read_csv("gs://{0}/cf_test_input/sensoryconfig/config2.csv".format(bucket_name),dtype=str)
    
    diff = len(config2)-len(config1)
    if diff<=0:
        return
    
    config = config2.tail(diff)
    file_list = config['Filename'].tolist()
    for file in file_list:
        filename = "gs://{0}/cf_test_input/sensory/{1}".format(bucket_name, file)
        if len(config1.where((config1['Filename'] == file))):
            remove_from_database(filename,"","sensory")

        metadata_local = define_sensory_metadata_table()
        data_local = define_sensory_data_table()

        try:
            file_extension = pathlib.Path(filename).suffix
            flag = 0
            sheets = []
        
            if file_extension == '.csv':
                flag=1
            else:
                xls = pd.ExcelFile(filename)
                sheets = xls.sheet_names
            
            sheet_no = 0
            while sheet_no<len(sheets) or flag==1:

                try:
                    if flag==1:
                        df = pd.read_csv(filename)
                        sheet = 'NA'
                        flag=0
                    else:
                        sheet = sheets[sheet_no]
                        sheet_no = sheet_no+1
                        df = pd.read_excel(filename, sheet_name=sheet)

                    project_cord = guess_row_col(df, "Project Name\\:")
                    fragrance_cord = guess_row_col(df, "Fragrance\\:")
                    batch_cord = guess_row_col(df, "SEU Ref")
                    setting_cord = guess_row_col(df, "Details")
                    rep_cord = guess_row_col(df, "Rep \\d")
                    format_cord = guess_row_col(df, "Format\\:")
                    mean_cord = guess_row_col(df, "Mean")
                    no_of_reps = len(rep_cord)

                    metadata_local = metadata_local.append({'Experiment_projectName' : get_data(df,project_cord), 
                                                            'Experiment_batch' : get_data(df,batch_cord),
                                                            'Fragrance_ID' : get_data(df,fragrance_cord),
                                                            'Device_settings' : get_data(df,setting_cord),
                                                            'Product_format' : get_data(df,format_cord),
                                                            'Experiment_dataType' : 'Sensory',
                                                            'Sheet_name': sheet, 
                                                            'File_name': filename},
                                                           ignore_index=True)

                    experiment_id = hashlib.md5(str(metadata_local).encode()).hexdigest()
                    metadata_local["Experiment_ID"] = experiment_id 

                    for x in range(0,len(rep_cord)):
                        if rep_cord[x][1] == 1:
                            product_row = rep_cord[x][0]+1
                            product_col = rep_cord[x][1]+1
                            break

                    for i in range(1,9):
                        product = str(df.iloc[product_row+i,product_col])
                        if product == '0' or product == None or product == 'nan':
                            no_of_products = i-1
                            break

                    for repetition in range(0,no_of_reps):
                        if rep_cord[repetition][1] == 1:
                            current_rep_row = rep_cord[repetition][0]

                            for i in range(1,no_of_products+1):
                                df1 = df
                                df1.fillna('', inplace = True)
                                significance = list(string.ascii_lowercase) + list(string.ascii_uppercase)
                                x = df1.iloc[current_rep_row+1+i, rep_cord[repetition][1]+3:rep_cord[repetition][1]+3+5]
                                x = ''.join([str(y.strip()) for y in x if(str(y.strip()) in significance)])

                                data_local = data_local.append({'Measurement_timestamp' : str(df.iloc[current_rep_row+2, rep_cord[repetition][1]]), 
                                                                'Experiment_batch' : str(df.iloc[batch_cord[0][0], batch_cord[0][1]+1]),
                                                                'Experiment_ID' : experiment_id,
                                                                'Measurement_product' : str(df.iloc[current_rep_row+1+i, rep_cord[repetition][1]+1]),
                                                                'Measurement_mean' : str(df.iloc[current_rep_row+1+i,rep_cord[repetition][1]+2]),
                                                                'Measurement_significance' : x,
                                                                'Measurement_repetition' : str(df.iloc[rep_cord[repetition][0],rep_cord[repetition][1]]),
                                                                'Sheet_name': sheet, 
                                                                'File_name': filename},
                                                                ignore_index=True)

                except Exception as e:
                    print(e) 

            final_data = pd.concat([final_data, data_local], axis = 0)
            final_metadata = pd.concat([final_metadata, metadata_local], axis = 0)
            print(f"-> {filename} ---- PARSED")    
        except Exception as e:
            print(f"{filename} :: {sheet} -> {e}") 
            
    final_data['Measurement_timestamp'] = final_data['Measurement_timestamp'].map(lambda x : change_to_date(x))
    final_data['Measurement_timestamp'] = final_data['Measurement_timestamp'].map(lambda x : x.strip())
    
    sensory_ingestion(final_data, final_metadata)
    config2.to_csv("gs://devrb-eumr-cs-rnd-edcam-poc1/cf_test_input/sensoryconfig/config1.csv", encoding='utf-8', index=False, header=True)
    print("ALL PROCESS COMPLETED!!")
    return

def convert_dict_values(dictionary):
    converted_dict = {}
    for key, value in dictionary.items():
        if isinstance(value, float) and np.isnan(value):
            converted_dict[key] = "NA"
        else:
            converted_dict[key] = str(value)
    return converted_dict

def define_clt_metadata_table():
    column_metadata = ["CLT_startdate",
    "CLT_length",
    "number_participants",
    "CLT_category",
    "Experiment_objective",
    "CLT_owner",
    "CLT_projectName",
    "Product_format",
    "File_name",
    "Product_description",
    "Fragrance_ID",
    "Measurement_location",
    "file_upload_date"]
    return pd.DataFrame(columns = column_metadata)

def define_clt_data_table():
    column_data = ["Measurement_Cell",
    "Participant_internalID",
    "Participant_otherID",
    "Survey_QID",
    "Question_answerGiven",
    "File_name"]
    
    return pd.DataFrame(columns = column_data)

def define_clt_master_table():
    column_metadata = ['File_name',
     'Survey_QID',
     'Question_Wording',
     'Question_internalID',
     'Number_of_response_options',
     'Question_type',
     'numerical_response_map'
    ]
    return pd.DataFrame(columns = column_metadata)

def clt_data_ingestion(data, data_meta, master_data):
    string_list = ["Measurement_Cell",
        "Experiment_ID",
        "Measurement_location",
        "Product_description",
        "Participant_ageCategory",
        "Participant_ageRange",
        "Participant_gender",
        "Participant_otherID",
        "Question_answerGiven",
        "Question_timePassed",
        "File_name",
        "Sheet_name",
        "Question_Wording",
        "Question_Response_Map",
        "Participant_internalID"]

    int_list = ["Participant_age", "Experiment_batch", "Survey_QID"]
    date_list = ["Measurement_timestamp"]
    string_list_meta = [
        "Experiment_batch",
        "Experiment_dataType",
        "Experiment_ID",
        "Experiment_laboratory",
        "Experiment_objective",
        "Experiment_onBehalf",
        "Experiment_owner",
        "Experiment_projectName",
        "Experiment_riskAssessmentCode",
        "Experiment_seriesNumber",
        "Experiment_SOPNumber",
        "Experiment_title",
        "Fragrance_ID",
        "Product_ID",
        "Product_description",
        "Product_format",
        "File_name",
        "Sheet_name"
    ]

    date_list_meta = ["Experiment_dateLastModified", "Experiment_startDate"]

    for x in string_list:
        data[x].fillna("NA", inplace = True)
    for x in int_list:
        data[x].fillna(0, inplace = True)
    for x in date_list:
        data[x].fillna(pd.Timestamp("19000101"),inplace = True) 
    for x in string_list_meta:
        data_meta[x].fillna("NA", inplace = True)
    for x in date_list_meta:
        data_meta[x].fillna(pd.Timestamp("19000101"),inplace = True)  
        
    data.columns = [x.lower() for x in data.columns]
    data_meta.columns = [x.lower() for x in data_meta.columns]

    dropList = ['device_number', 'device_batteryinstallationdate']
    data.drop(dropList, axis = 1, inplace = True)
    data['fragrance_id'] = 'NA'

    ## MASTER DATA
    master = master_data
    l = list(master.columns)
    l.remove('Number_of_response_options')

    for x in l:
        if x!='numerical_response_map':
            master[x].fillna("NA", inplace = True)
    master['Number_of_response_options'].fillna(0,inplace = True)
    master['numerical_response_map'].fillna('{}',inplace = True)
    master['Number_of_response_options'] = master['Number_of_response_options'].astype('int64')
    master.columns = [x.lower() for x in master.columns]
    ## END

    db = sqlalchemy.create_engine(
            "postgresql+pg8000://",
            creator=getconn,
            pool_pre_ping=True
        )
    
    print("DATA INGESTION PROCESS STARTED...")
    
    data.to_sql('clt_data_cf', con = db, index=False, if_exists = 'append', chunksize = 1000, method='multi')
    data_meta.to_sql('clt_metadata_cf', con = db, index=False, if_exists = 'append', chunksize = 1000, method='multi')
    print("DATA INGESTION PROCESS FINISHED!!")
    #db.close()
    return


def clt_parser():
    final_metadata = define_clt_metadata_table()
    final_data = define_clt_data_table()

    cltconfig1 = pd.read_csv("gs://{0}/cf_test_input/clt_config/clt/cltconfig1.csv".format(bucket_name),dtype=str)
    cltconfig2 = pd.read_csv("gs://{0}/cf_test_input/clt_config/clt/cltconfig2.csv".format(bucket_name),dtype=str)

    diff = len(cltconfig2)-len(cltconfig1)
    cltconfig = cltconfig2.tail(diff)

    if diff<=0:
       return

    cltconfig.columns = ["Filename", "Participant_internalID", "Product_description", "Participant_otherID", "Measurement_location", "Question_timePassed", "Measurement_setID", "Sheet_name"]
    cltconfigcols = cltconfig.columns

    for row in range(0,len(cltconfig)):#len(cltconfig)
        file = cltconfig.iloc[row].Filename
        filename = f"gs://{bucket_name}/databricks_run/clt/{file}"

        file_extension = pathlib.Path(filename).suffix
        flag = 0
        sheets = str(cltconfig.iloc[row].Sheet_name).split(',')

        if file_extension == '.csv':
            flag = 1
            sheets.clear()
        
        no = 0
        while no<len(sheets) or flag==1:
            try :
                if flag == 0:
                    sheet = sheets[no]
                    no=no+1
                    df = pd.read_excel(filename, sheet_name=sheet)
                else:
                    df = pd.read_csv(filename)
                    sheet = 'NA'
                    flag=0

                if len(cltconfig1.where((cltconfig1['Filename'] == file) & (cltconfig1['Sheet_name'] == sheet))):
                   remove_from_database(filename,sheet,"clt")

                df = pd.read_excel(filename, sheet_name=sheet)
                df.columns = df.columns.astype(str)
                columns = df.columns

                for x in columns:
                    if "Unnamed" in x:
                        df = df.drop([f'{x}'], axis = 1)    

                a_set = set(cltconfig.iloc[row].values)
                b_set = set(columns)

                if len(a_set.intersection(b_set))>0:
                    metadata_local = define_clt_metadata_table()

                    project_name = filename.replace("gs://devrb-eumr-cs-rnd-edcam-poc1/databricks_run/clt/","")
                    file = project_name
                    project_name = project_name.replace(".xls","")

                    ###CODE FOR MASTER DATA
                    df = df.drop(index=[0]).reset_index(drop=True)
                
                    df.loc[-1] = df.columns
                    df.index = df.index + 1
                    df = df.sort_index()
                    
                    first_three_rows = df.iloc[:3]
                    new_df = pd.DataFrame()
                    new_df['Survey_QID'], new_df['Question_Wording'], new_df['Question_internalID'] = np.array_split(first_three_rows.values.flatten(), 3)
                    new_df['File_name'] = filename
                    
                    master_data = pd.concat([master_data, new_df], axis = 0)
                    df = df.drop(index=[0, 1, 2]).reset_index(drop=True)
                    ##END

                    filecolumns = list([item for item in cltconfig.iloc[row].values])
                    meltcolumns = list(filecolumns.index(col) for col in filecolumns if not(pd.isnull(col)) == True)
                    idvars = list([filecolumns[x] for x in meltcolumns if x != 5 and x!=0 and x!=7])

                    df.rename(columns=lambda x: x.strip(), inplace = True)

                    final_format = pd.melt(df, id_vars = idvars)

                    metadata_local = metadata_local.append({'Experiment_projectName' : project_name, 
                                                            'Experiment_dataType': "CLT",
                                                            'Sheet_name': sheet, 
                                                            'File_name': filename}, 
                                     ignore_index=True)
                    experiment_id = hashlib.md5(str(metadata_local).encode()).hexdigest()

                    metadata_local["Experiment_ID"] = experiment_id

                    i = j = 0
                    final_col = final_format.columns
                    for column in cltconfigcols:
                        if final_col[j] == filecolumns[i]: 
                            final_format.rename(columns = {final_col[j]: column}, inplace = True)
                            j = j+1
                        i = i+1

                    final_format["Participant_internalID"] = final_format["Participant_internalID"].astype(str)
                    try : final_format["Measurement_setID"] = final_format["Measurement_setID"].astype(str) 
                    except Exception: pass
                    try : final_format["Product_description"] = final_format["Product_description"].astype(str)
                    except Exception: pass
                    try : final_format["Measurement_location"] = final_format["Measurement_location"].astype(str)
                    except Exception: pass

                    final_format.rename(columns = {"variable" : "Question_internalID"}, inplace = True)
                    final_format.rename(columns = {"value" : "Question_answerGiven"}, inplace = True)

                    final_format["Experiment_ID"] = experiment_id
                    final_format["File_name"] = filename
                    final_format["Sheet_name"] = sheet

                    final_data = pd.concat([final_data, final_format], axis = 0)
                    final_metadata = pd.concat([final_metadata, metadata_local], axis = 0)
                    print(f"{row} {file} - {sheet}")

                    #Releasing memory
                    del final_format
                    del metadata_local
                    #break
            except Exception as e: 
                print(f"{row}-{file} - {sheet} -   Error : {e}")

    drop_list = ['Device_settings','Device_name', 'Question_internalID', 'Question_type', 'Question_ID', 'Question_content']
    final_metadata = final_metadata.drop(drop_list, axis = 1)
    final_data.rename(columns = {'Measurement_setID':'Measurement_Cell'}, inplace = True)

    final_data['Survey_QID'] = 0
    final_data['Question_Wording'] = ''
    final_data['Question_Response_Map'] = ''

    clt_data_ingestion(final_data,final_metadata, master_data)
    #Releasing memory 
    del final_data
    del final_metadata
    cltconfig2.to_csv("gs://devrb-eumr-cs-rnd-edcam-poc1/cf_test_input/cltconfig/cltconfig1.csv", encoding='utf-8', index=False, header=True)
    return 


## CODE FOR HUT NEW
def define_hut_master_table():
    column_metadata = ['Filename',
     'Survey_QID',
     'Question_Wording',
     'Question_internalID',
     'Number_of_response_options',
     'Question_type',
     'numerical_response_map'
    ]
    return pd.DataFrame(columns = column_metadata)

def define_hut_metadata_table():
    column_metadata = ["HUT_startdate",
    "HUT_length",
    "number_participants",
    "HUT_category",
    "Experiment_objective",
    "HUT_owner",
    "HUT_projectName",
    "Product_format",
    "File_name",
    "Product_description",
    "Fragrance_ID",
    "Measurement_location",
    "file_upload_date"]
    return pd.DataFrame(columns = column_metadata)

def define_hut_data_table():
    column_data = ["Measurement_Cell",
    "Participant_internalID",
    "Participant_otherID",
    "Survey_QID",
    "Question_answerGiven",
    "File_name"]
    
    return pd.DataFrame(columns = column_data)

def define_hut_consumerID_metadata_table():
    column_data = ["Participant_age",
    "participant_agecategory",
    "participant_agerange",
    "participant_gender",
    "Participant_location"
    ]
    
    return pd.DataFrame(columns = column_data)


def hut_data_ingestion(data, data_meta, master):    
    string_list = ["Measurement_Cell",
        "Participant_otherID",
        "Question_answerGiven",
        "File_name",
        "Participant_internalID",
        "Survey_QID"]

    string_list_meta = [
        "HUT_category",
        "Experiment_objective",
        "HUT_owner",
        "HUT_projectName",
        "Product_format",
        "File_name",
        "Product_description",
        "Fragrance_ID",
        "Measurement_location"]

    int_list_meta = ["HUT_length", "number_participants"]
    date_list_meta = ["HUT_startdate", "file_upload_date"]

    for x in string_list:
        data[x].fillna("NA", inplace = True)
    for x in string_list_meta:
        data_meta[x].fillna("NA", inplace = True)
    for x in date_list_meta:
        data_meta[x].fillna(pd.Timestamp("19000101"),inplace = True)  
        data_meta[x]= pd.to_datetime(data_meta[x])
    for x in int_list_meta:
        data_meta[x].fillna(0, inplace = True)
        data_meta[x] = data_meta[x].astype(int)

    if 'Measurement_location' in data.columns:
        dropList = ['Measurement_location']
        data = data.drop(dropList, axis = 1)
    data.columns = [x.lower() for x in data.columns]
    data_meta.columns = [x.lower() for x in data_meta.columns]


    ##Master data
    l = list(master.columns)
    l.remove('Number_of_response_options')
    l.remove('numerical_response_map')

    for x in l:
        master[x].fillna("NA", inplace = True)
    master['Number_of_response_options'].fillna(0,inplace = True)
    master['Number_of_response_options'] = master['Number_of_response_options'].astype('int64')
    master["numerical_response_map"].fillna("{}", inplace = True)
    master.columns = [x.lower() for x in master.columns]

    db = sqlalchemy.create_engine(
            "postgresql+pg8000://",
            creator=getconn,
            pool_pre_ping=True
        )
    
    print("DATA INGESTION PROCESS STARTED...")
    data.to_sql('hut_data_labelled_updated', con = db, index=False, if_exists = 'append', chunksize = 1000, method='multi')
    data_meta.to_sql('hut_metadata_labelled_updated', con = db, index=False, if_exists = 'append', chunksize = 1000, method='multi', dtype={'product_description': sqlalchemy.types.JSON,'fragrance_id': sqlalchemy.types.JSON})
    master.to_sql('hut_master_data_updated', con = db, index=False, if_exists = 'append', chunksize = 1000, method='multi',dtype={'numerical_response_map': sqlalchemy.types.JSON})
    print("DATA INGESTION PROCESS FINISHED!!")

def hut_parser():
    final_metadata = define_hut_metadata_table()
    final_data = define_hut_data_table()
    master_data = define_hut_master_table()
    hutconfig1 = pd.read_csv("gs://{0}/cf_test_input/hutconfig/hutconfig1.csv".format(bucket_name),dtype=str)
    hutconfig2 = pd.read_csv("gs://{0}/cf_test_input/hutconfig/hutconfig2.csv".format(bucket_name),dtype=str)

    diff = len(hutconfig2)-len(hutconfig1)
    hutconfig = hutconfig2.tail(diff)
    
    if diff<=0:
        return

    hutconfig.columns = ["Filename", "Participant_internalID", "Product_description", "Participant_otherID", "Measurement_location", "Question_timePassed", "Measurement_setID", "Sheet_name"]
    hutconfigcols = hutconfig.columns

    for row in range(0, len(hutconfig)):#len(hutconfig)
        file = hutconfig.iloc[row].Filename
        filename = "gs://{0}/cf_test_input/{1}".format(bucket_name,file)
        
        #checking extension of file
        file_extension = pathlib.Path(filename).suffix
        flag = 0
        
        sheets = str(hutconfig.iloc[row].Sheet_name).split(',')
        
        if file_extension == '.csv':
            flag = 1
            sheets.clear()
            
        no = 0
        while no<len(sheets) or flag == 1:
            if flag == 0:
                sheet = sheets[no]
                df = pd.read_excel(filename, sheet_name=sheet, dtype = str)
                no=no+1
            else:
                df = pd.read_csv(filename,dtype = str)
                sheet = 'NA'
                flag = 0

            if len(hutconfig1.where((hutconfig1['Filename'] == file) & (hutconfig1['Sheet_name'] == sheet))):
                remove_from_database(filename,sheet,"hut")   
                
            columns = df.columns
        
            for x in columns:
                if "Unnamed" in x:
                    df = df.drop([f'{x}'], axis = 1)
                
            a_set = set(hutconfig.iloc[row].values)
            b_set = set(columns)
            
            if len(a_set.intersection(b_set))>0:
                
                metadata_local = define_hut_metadata_table()

                project_name = filename.replace(f"gs://{bucket_name}/databricks_run/hut/","")
                file = project_name
                project_name = project_name.replace(file_extension,"")
                
                filename = filename.replace(f"gs://{bucket_name}/databricks_run/hut/","")
                
                ###CODE FOR MASTER DATA
                df.loc[-1] = df.columns
                df.index = df.index + 1
                df = df.sort_index()
                
                first_three_rows = df.iloc[:3]
                new_df = pd.DataFrame()
                new_df['Survey_QID'], new_df['Question_Wording'], new_df['Question_internalID'] = np.array_split(first_three_rows.values.flatten(), 3)
                new_df['Filename'] = filename
                master_data = pd.concat([master_data, new_df], axis = 0)
                
                df = df.drop(index=[0, 2]).reset_index(drop=True)
                ###END
                
                filecolumns = list([item for item in hutconfig.iloc[row].values])
                meltcolumns = list(filecolumns.index(col) for col in filecolumns if not(pd.isnull(col)) == True)
                idvars = list([filecolumns[x] for x in meltcolumns if x != 5 and x!=0 and x!=7])
                
                df.rename(columns=lambda x: x.strip(), inplace = True)
                
                final_format = pd.melt(df, id_vars = idvars)

                for x in final_format.columns:
                    final_format[x] = final_format[x].astype(str)

                # CODE CHANGE FOR METADATA_LOCAL PRODUCT_DISCRIPTION AND FRANGRANCE_ID ADDED AS DICTIONARY
                unique_pairs = df.drop_duplicates(subset=['CELL', 'QPRODUCT'])
                product_discription_dict = unique_pairs.set_index('CELL')['QPRODUCT'].to_dict()
                
                fragrance_id_dict = {k: 'NA' for k in unique_pairs['CELL']}
                product_discription_dict = convert_dict_values(product_discription_dict)
                fragrance_id_dict = convert_dict_values(fragrance_id_dict)
                
                metadata_local = metadata_local.append({'HUT_projectName' : project_name, 
                                        'File_name': filename,
                                        'Product_description' : dict(product_discription_dict),
                                        'Fragrance_ID' : dict(fragrance_id_dict),
                                        'Measurement_location': "US",
                                        'file_upload_date':datetime.datetime.now()},
                                ignore_index=True)
                # END
                
                i = j = 0
                final_col = final_format.columns
            
                for column in hutconfigcols:
                    if final_col[j] == filecolumns[i]: 
                        final_format.rename(columns = {final_col[j]: column}, inplace = True)
                        j = j+1
                    i = i+1
                
                final_format["Participant_internalID"] = final_format["Participant_internalID"].astype(str)
                try : final_format["Measurement_Cell"] = final_format["Measurement_Cell"].astype(str) 
                except Exception: pass
                
                final_format.rename(columns = {"variable" : "Survey_QID"}, inplace = True)
                final_format.rename(columns = {"value" : "Question_answerGiven"}, inplace = True)
                
                final_format["File_name"] = filename
                
                final_data = pd.concat([final_data, final_format], axis = 0)
                final_metadata = pd.concat([final_metadata, metadata_local], axis = 0)
                print(f"{row} {file} - {sheet}")
                #break
            
    hut_data_ingestion(final_data,final_metadata, master_data)
    #Releasing memory
    del final_data
    del final_metadata

    hutconfig2.to_csv(f"gs://{bucket_name}/cf_test_input/hutconfig/hutconfig1.csv", encoding='utf-8', index=False, header=True)
    print("PROCESS COMPLETE !!")
    return
        

# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def hello_gcs(cloud_event):
   data = cloud_event.data

   event_id = cloud_event["id"]
   event_type = cloud_event["type"]

   bucket = data["bucket"]
   filename = data["name"]
   metageneration = data["metageneration"]
   timeCreated = data["timeCreated"]
   updated = data["updated"]

   print(f"Event ID: {event_id}")
   print(f"Event type: {event_type}")
   print(f"Bucket: {bucket}")
   print(f"File: {filename}")
   print(f"Metageneration: {metageneration}")
   print(f"Created: {timeCreated}")
   print(f"Updated: {updated}")

   ##TODO
   file_type = filename.split('/')[1]

   if file_type == 'hutconfig':
       try:
           hut_parser()
       except Exception as e:
           send_mail(e, 'hut')

   elif file_type == 'clt_config':
        try:
           clt_parser()
        except Exception as e:
           send_mail(e, 'clt')

   elif file_type == 'sensoryconfig':
       try:
           sensory_parser()
       except Exception as e:
           send_mail(e, 'sensory')

   else:
       try:
           hut_parser()
           clt_parser()
           sensory_parser()
       except Exception as e:
           send_mail('all', e)

   return '{0} data is populated, the bucket is {1}!\n'.format(file_type, bucket_name)
