# 3rd party
import json
import pandas as pd
# Owned
from relic_gemd.common.documentdb.load_firecracker_to_documentdb import documentdb_connect


def transform_measurement_run(rows):
    """It will make a call to measurement_run function for every data row"""
    null = None
    c = 0
    global df_main, df_sub, df_new, df_cond
    # Iterating through each row from dictionary
    for item in rows:
        data = (item if item else None) if 1 else item

        # Capturing values for various measurement_run json fields
        dsjson = json.loads(data['results'])
        dsjson_title = json.loads(str(data['name']))
        dsjson_id = json.loads(str(data['id']))
        dsjson_material = json.loads(str(data['material_id']))
        dsjson_temp = json.loads(str(data['temp_id']))
        dsjson_tag = data['tags'] if data['tags'] is None else json.loads(str(data['tags']))

        """" Variables/Dataframes used in measurement run data transformation for properties and conditions
             1.path_main is the main variable to concatenate sub paths,path_sub and path_sub1 intermediate ones
             2.df_main is the main dataframe where we append sub data such as df_sub
        """
        path_main = []
        level = 0
        df_main = pd.DataFrame()
        df_sub = pd.DataFrame()
        df_new = pd.DataFrame()
        df_cond = pd.DataFrame()

        measurement_run(dsjson, path_main, level)

        df_main = df_main.reset_index()
        df_main.drop(columns="index", inplace=True)
        df_main['file_links'] = [[] for _ in range(len(df_main))]
        df_main['notes'] = null
        df_main['origin'] = "measured"
        df_main['type'] = "property"
        df_main['template'] = [{"id": dsjson_temp,
                                "scope": "skylab-template", "type": "link_by_uid"} for _ in range(len(df_main))]
        # declaring final dictionary
        df_final = {}
        if len(df_cond) > 0:
            df_cond['file_links'] = [[] for _ in range(len(df_cond))]
            df_cond['notes'] = null
            df_cond['origin'] = "specified"
            df_cond['template'] = df_main['template']
            df_cond['type'] = 'condition'
            df_cond = df_cond.to_dict('records')
        else:
            df_cond = []

        df_main = df_main.to_dict(orient='records')
        # Main GEMD elements
        df_final['name'] = "Measurement run for " + dsjson_title
        df_final['notes'] = "Measurement run for " + dsjson_title
        df_final['file_links'] = {
            "filename": null,  # dsjson_title['original_filename'],
            "type": "file_link",
            "url": null  # dsjson_title['path']  # needs to be changed
        }
        df_final['material'] = {
            "id": dsjson_material,
            "scope": "skylab",
            "type": "link_by_uid"
        }

        df_final['parameters'] = []
        df_final['conditions'] = df_cond if len(df_cond) > 0 else []
        df_final['properties'] = df_main
        df_final["spec"] = {
            "id": null,
            "scope": "skylab",
            "type": "link_by_uid"
        }
        df_final["source"] = null
        df_final['tags'] = dsjson_tag
        df_final['type'] = "measurement_run"
        df_final["uids"] = {
            "skylab": dsjson_id
        }
        df_out = {"data": df_final}
        print('Loading data into measurement_run')
        documentdb_connect(df_out, 'measurement_run', 'skylab')
        c = c + 1


def measurement_run(dict1, path_main, level):
    """This is the main function to transform measurement/test data
             """

    global df_main, df_cond, df_sub, df_new
    a_key = dict1.keys()
    level = level + 1

    # This logic will traverse through all nested elements for properties and conditions
    for k in a_key:
        if k == 'metadata' or k == 'resource_metadata':
            cond = dict1[k]
            p = path_main
            if k == 'resource_metadata':
                p.append(k)
                for val in cond:
                    p.append(val)
                    for key in cond[val]:
                        df_new['name'] = pd.Series('->'.join(p) + '->' + key)
                        df_new['value'] = cond[val][key]
                        df_cond = df_cond.append(df_new)

            else:
                for val in cond:
                    df_new['name'] = pd.Series(k + '->' + val)
                    df_new['value'] = [cond[val] for _ in range(len(df_new))]
                    df_cond = df_cond.append(df_new)
            continue
        subnode = dict1[k]
        if k not in path_main:
            path_main.append(k)
        if isinstance(subnode, dict):
            if 'unit' not in subnode.keys():
                measurement_run(subnode, path_main, level)
            else:
                path_sub = ' -> '.join(path_main)
                df_sub['name'] = pd.Series(path_sub)
                df_sub['value'] = [subnode for _ in range(len(df_sub))]
                df_main = df_main.append(df_sub)
            if len(path_main) > (level - 1):
                del path_main[-1]
            else:
                del path_main[-level]
        elif type(subnode) is list:
            i = 0
            for a in subnode:
                if isinstance(a, dict):
                    measurement_run(a, path_main, level)
                    i = i + 1
            if i == 0:
                # it doesn't contain dict and should not be in loop
                path_sub1 = ' -> '.join(path_main)
                df_sub['name'] = pd.Series(path_sub1)
                df_sub['value'] = [subnode for _ in range(len(df_sub))]
                df_main = df_main.append(df_sub)
                del path_main[-1]
        else:
            path_sub1 = ' -> '.join(path_main)
            del path_main[-1]
            df_sub['name'] = pd.Series(path_sub1)
            df_sub['value'] = subnode
            df_main = df_main.append(df_sub)
