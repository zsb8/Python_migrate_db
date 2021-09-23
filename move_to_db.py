#-*-coding:utf-8-*-
import pandas as pd
from pandas import Series
import datetime
import time
from utils import pgfunctions as pg
from utils import other_functions as oth


def find_new_data(df1, df2):
    result = pd.merge(df1, df2, on=['first_name', 'last_name', 'title'], how="left")
    if result["test"].notnull().any():
        return 1, df1
    else:
        result.drop(result[pd.notnull(result['test'])].index, inplace=True)
        result.rename(columns={'clientele_groups': 'client_group'} , inplace=True)
        result.rename(columns={'reasons': 'expertise'}, inplace=True)
        result["created_at"] = datetime.datetime.today()
        result["updated_at"] = datetime.datetime.today()
        result.index = Series(range(len(result)))
        return 0, result


def retrieve_unique_columns(df, column_list):
    """to retrieve one dataframe that only includes the unique value of group by column_list
       para: df--full dataframe that has duplicate value
              column_list: you like to let this columns combination value is unique,
              like: [['province','city', 'address;, 'postcode']
       return: dataframe that has only unique value group by column list
              """
    df_unique = df.groupby(by=column_list, as_index=False).count()
    df_unique = df_unique[column_list]
    return df_unique


def delete_null_row(df):
    column_list = ['name', 'location_id']
    df_new = df.groupby(by=column_list, as_index=False).agg('count')
    name_list = []
    locate_list = []
    for i in range(0, len(df_new)):
        if df_new['workplace'][i] > 1:
            name = df_new['name'][i]
            locate = df_new['location_id'][i]
            name_list.append(name)
            locate_list.append(locate)
    for i in range(0, len(name_list)):
        df.drop(index=df[(df['name'] == name_list[i]) & (df['location_id'] == locate_list[i])
                          & (df['workplace'] == '')].index, inplace=True)
    return df


def org_import(temp_df_all):
    # too complex, so set this function to deal with alone
    temp_df_all['address'] = temp_df_all['coords'].apply(lambda x: oth.separate_addrss(x)[0].strip())
    temp_df_all['city'] = temp_df_all['coords'].apply(lambda x: oth.separate_addrss(x)[1].strip())
    temp_df_all['province'] = temp_df_all['coords'].apply(lambda x: oth.separate_addrss(x)[2].strip())
    temp_df_all['postal_code'] = temp_df_all['coords'].apply(lambda x: oth.separate_addrss(x)[3].strip())
    temp_df_all.rename(columns={'lon': 'lng', 'user_id': 'owner_id', 'organizations': 'name'}, inplace=True)
    temp_df_all['name'] = temp_df_all['name'].apply(lambda x: x.strip())
    # compare location between two df
    on_column = ['address', 'city', 'province', 'postal_code', 'lat', 'lng']
    df_distinct = retrieve_unique_columns(temp_df_all, on_column)
    df_new = pg.get_new_df("location", df_distinct, on_column)
    df_new = df_new.sort_values("address")
    # import new location data to location table
    pg.df_columns_db("location", df_new, on_column)
    # get the all location info from table and add location_id to the df_merge
    my_sql = "select id,address,city,province,postal_code,lat,lng from location"
    df_location_all = pg.database_to_pd(my_sql)
    df_merge = pd.merge(temp_df_all, df_location_all, on=on_column)
    df_merge.rename(columns={'id': 'location_id'}, inplace=True)
    # import new organization data to organization table
    column_org = ['name', 'location_id', 'workplace', 'profile']
    df_distinct_org = retrieve_unique_columns(df_merge, column_org)
    df_new_org = pg.get_new_df("organization", df_distinct_org, column_org)
    df_new_org = delete_null_row(df_new_org)
    df_new_org = df_new_org.sort_values("name")
    pg.df_columns_db("organization", df_new_org, column_org)
    # get the all organization info from table
    my_sql = "select id,name,location_id,workplace,profile from organization"
    df_org_all = pg.database_to_pd(my_sql)
    column_org = ['name', 'location_id']
    df_merge = pd.merge(df_merge, df_org_all, on=column_org)
    df_merge.rename(columns={'id': 'org_id'}, inplace=True)
    # import new physio_org data to physio_org table
    column_physio_org = ['physio_id', 'org_id']
    df_distinct = retrieve_unique_columns(df_merge, column_physio_org)
    df_distinct = df_distinct.sort_values("physio_id")
    pg.df_columns_db("physio_org", df_distinct, column_physio_org)


def utils(df, col, layer):
    begin_time = time.time()
    columns_list = [col, 'user_id', 'physio_id', 'created_at', 'updated_at']
    my_df = df.loc[:, columns_list]
    if layer == 1:
        if col == "client_group":
            table_name = 'physio_client_type'
            temp_cols = ['client_group', 'physio_id', 'created_at', 'updated_at']
        if col == "languages":
            table_name = 'physio_language'
            temp_cols = ['language', 'physio_id', 'created_at', 'updated_at']
        if col == "approaches":
            table_name = 'physio_approach'
            temp_cols = ['approach', 'physio_id', 'created_at', 'updated_at']
        if col == "expertise":
            table_name = 'physio_expertise'
            temp_cols = ['expertise', 'physio_id', 'created_at', 'updated_at']
        temp_df_all = pd.DataFrame(columns=temp_cols)
    if layer == 2 and col == "organizations":
        temp_cols = ['name', 'workplace', 'coords', 'profile', 'lat', 'lon']
        temp_df_all = pd.DataFrame(columns=temp_cols)
        table_name = 'physio_org'
    for row in my_df.itertuples():
        data = getattr(row, col)
        if (type(data) is list) and len(data) > 0:
            u_id = getattr(row, 'user_id')
            p_id = getattr(row, 'physio_id')
            created_at = getattr(row, 'created_at')
            updated_at = getattr(row, 'updated_at')
            if layer == 1:
                cols = [col]
                temp_df = pd.DataFrame(data=data, columns=cols)
                temp_df['physio_id'] = p_id
                temp_df['created_at'] = created_at
                temp_df['updated_at'] = updated_at
                temp_df.columns = temp_cols
            if layer == 2 and col == "organizations":
                cols = list(data[0].keys())
                temp_df = pd.DataFrame(data=data, columns=cols)
                temp_df['user_id'] = u_id
                temp_df['physio_id'] = p_id
                temp_df['created_at'] = created_at
                temp_df['updated_at'] = updated_at
        temp_df_all = pd.concat([temp_df_all, temp_df], ignore_index=True)
    if layer == 2 and col == "organizations":
        org_import(temp_df_all)

    if layer == 1:
        if col == "client_group":
            temp_df_all["client_group"] = temp_df_all["client_group"].apply(lambda x: x.strip())
        if col == "languages":
            temp_df_all["language"] = temp_df_all["language"].apply(lambda x: x.strip())
        if col == "approaches":
            temp_df_all["approach"] = temp_df_all["approach"].apply(lambda x: x.strip())
        if col == "expertise":
            temp_df_all["expertise"] = temp_df_all["expertise"].apply(lambda x: x.strip())
        pg.df_columns_db(table_name, temp_df_all, temp_cols)
    end_time = time.time()
    print(f"Cost :{round(end_time - begin_time, 2)} seconds.")

begin_time = time.time()
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

with open('physios_renamed.json', mode='r', encoding='UTF-8') as file:
    myJson = file.read()
    file.close()
df = pd.read_json(myJson).T

column_length = df.shape[0]
df["p_name"] = df.index
df["p_name"] = df["p_name"].apply(lambda x: x.strip())
df.index = Series(range(column_length))

print("Delete null row.")
df.drop(df[pd.isnull(df['title'])].index, inplace=True)
print("Separate name in Json to firstName and lastName. ")
df_name = oth.separate_name(df, "p_name")
print("Begin to compare.")
sql = """
    select c.first_name, c.last_name, d.title
"""
sql = """ select c.first_name,c.last_name,d.title,'zsb' as test from heka_user as c \
inner join (select a.user_id,b.title from physio as a \
inner join physio_title as b on a.id=b.physio_id) as d on c.id=d.user_id;   """
df2 = pg.database_to_pd(sql)
result = find_new_data(df_name, df2)
if result[0] == 1:
    print("Not any new data need to import!")
else:
    print("Begin to add user_id and physio_id.")
    df = result[1]

    max_id_client = pg.max_column("heka_user", "id")
    id_add_client = list(range(max_id_client+1, max_id_client+1+len(df.index)))
    df.insert(0, "user_id", id_add_client)
    max_id_physios = pg.max_column("physio", "id")
    id_add_physios = list(range(max_id_physios+1, max_id_physios+1+len(df.index)))
    df.insert(1, "physio_id", id_add_physios)
    df["title"] = df["title"].apply(lambda x: x.strip())

    print("Begin to insert df to heka_user table.")
    myDf_Clients = df.iloc[:, [0, 9, 10, 12, 13]]  # user_id, first_name, last_name, created_at, updated_at
    myDf_Clients2 = myDf_Clients.rename(columns={'user_id': 'id'})
    myDf_Clients2["type_code"] = 3
    columns = ['id', 'first_name', 'last_name', 'created_at', 'updated_at', 'type_code']
    pg.df_columns_db("heka_user", myDf_Clients2, columns)
    print("OK! Insert df to heka_user table completely.")

    print("Begin to insert df to physio table.")
    myDf_PhysioId = df.iloc[:, [1, 0, 12, 13]]  # physio_id, user_id, created_at, updated_at
    myDf_PhysioId2 = myDf_PhysioId.rename(columns={'physio_id': 'id'})
    columns = ['id', 'user_id',  'created_at', 'updated_at']
    pg.df_columns_db("physio", myDf_PhysioId2, columns)
    print("OK! insert df to physio table completely.")

    print("Begin to insert df to location, organization, physio_orgtable.")
    column = "organizations"
    layer = 2
    utils(df, column, layer)
    print("OK! Insert physio_org and location table  completely.")

    print("Begin to insert df to physio_title table.")
    myDf_PhysiosTitles = df.iloc[:, [1, 2, 12, 13]]  # physio_id, title, created_at, updated_at
    columns = ['physio_id', 'title', 'created_at', 'updated_at']
    pg.df_columns_db('physio_title', myDf_PhysiosTitles, columns)
    print("OK! insert df to physio_title table completely.")

    print("Begin to insert df to physio_client_type table.")
    column = "client_group"
    layer = 1
    utils(df, column, layer)
    print("OK! Insert physio_client_type table completely.")

    print("Begin to insert df to languages table.")
    column = "languages"
    layer = 1
    utils(df, column, layer)
    print("OK! Insert languages table completely.")

    print("Begin to insert df to physio_expertise table.")
    column = "expertise"
    layer = 1
    utils(df, column, layer)
    print("OK! Insert physio_expertise table completely.")

    print("Begin to insert df to physio_approach table.")
    column = "approaches"
    layer = 1
    utils(df, column, layer)
    print("OK! Insert physio_approach table completely.")

end_time = time.time()
print(f"All Cost :{round(end_time-begin_time, 2)} seconds.")