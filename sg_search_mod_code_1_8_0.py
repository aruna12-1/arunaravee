import json
import patterns
import requests
import pandas as pd
from pandas import DataFrame
from requests.models import PreparedRequest
import command
import numpy as np
import matplotlib.pyplot as plt;
from nltk import flatten

plt.rcdefaults()
import numpy as np
import matplotlib.pyplot as plt
import re


##validations for user_input_year
def get_usr_year():
    while True:
        try:
            # Note: Python 2.x users should use raw_input, the equivalent of 3.x's input
            usr_yr_ip = int(input("Enter the Year:(YYYY) [1957 - 2019]: "))
            if ((usr_yr_ip > 1956 and usr_yr_ip < 2020) != True):
                continue

        except ValueError:
            print("Sorry, I didn't understand that.")
            # better try again... Return to the start of the loop
            continue
        else:
            # age was successfully parsed!
            # we're ready to exit the loop.
            break
    return usr_yr_ip


##validations for user_input_gender
def get_usr_gender():
    while True:
        try:
            # Note: Python 2.x users should use raw_input, the equivalent of 3.x's input
            usr_gen_ip = input("Enter the Gender[Male or Female or All]: ")
            if ((usr_gen_ip.upper() == "MALE") or (usr_gen_ip.upper() == "FEMALE") or (
                    usr_gen_ip.upper() == "ALL")) != True:
                continue
        except ValueError:
            print("Sorry, I didn't understand that.")
            # better try again... Return to the start of the loop
            continue
        else:
            # uccessfully parsed!
            # we're ready to exit the loop.
            break
    return usr_gen_ip


##validations for user_input_ethincs
def get_usr_ethinicity():
    while True:
        try:
            # Note: Python 2.x users should use raw_input, the equivalent of 3.x's input
            usr_ethy_ip = input("Enter the Ethinicity [Chinese, Indians, Malays, Other]: ")
            if ((usr_ethy_ip.upper() == "CHINESE") or (usr_ethy_ip.upper() == "INDIANS") or (
                    usr_ethy_ip.upper() == "MALAYS") or (usr_ethy_ip.upper() == "OTHER")) != True:
                continue
        except ValueError:
            print("Sorry, I didn't understand that.")
            # better try again... Return to the start of the loop
            continue
        else:
            # uccessfully parsed!
            # we're ready to exit the loop.
            break
    return usr_ethy_ip


usr_year = int(get_usr_year())
usr_gender = get_usr_gender().title()
usr_ethinic_grp = get_usr_ethinicity().title()

##getting request data from web url
req = PreparedRequest()
url = "https://data.gov.sg/api/action/datastore_search?resource_id=f9dbfc75-a2dc-42af-9f50-425e4107ae84"
params = {'q': usr_year}
req.prepare_url(url, params)

resp = requests.get(req.url)

# Convert JSON into Python Object
data = json.loads(resp.content)["result"]
results = []

# Loading json data to data Frames
df_graph = pd.DataFrame(data['records'])
# print(df_graph)
#
# #selecting specific fields for DataFrame - df_sg_popln_agg
df_sg_popln_agg = DataFrame(df_graph, columns=['year', 'level_1', 'level_2', 'value'])

##search condn for user gender
search_cond = " " + usr_gender

if usr_gender != 'All':
    df_mask = df_sg_popln_agg[df_sg_popln_agg['level_1'].str.contains(search_cond, na=False)]
else:
    df_mask = df_sg_popln_agg

print("Filtered Gender count : ", df_mask['level_1'].count())

print("Filtered Gender Data : ", df_mask, sep="\n")

##search condn for user_ethnics
search_cond = " " + usr_ethinic_grp
df_eth_mask = df_sg_popln_agg[df_sg_popln_agg['level_1'].str.contains(search_cond, na=False)]

count_df_eth_mask = df_eth_mask['level_1'].count()

if count_df_eth_mask > 0:
    print(" Filtered Ethincs  Count :", count_df_eth_mask)
    print(" Filtered Ethincs Data :", df_eth_mask, sep="\n")
else:
    print(" Ethinicity Group wise Data breakup not available ")

##search condn for user ethnics and user gender
search_cond = usr_gender + " " + usr_ethinic_grp

df_eth_grp_mask = df_sg_popln_agg[df_sg_popln_agg['level_1'].str.contains(search_cond, na=False)]

count_df_eth_grp_mask = df_eth_grp_mask['level_1'].count()

if count_df_eth_grp_mask > 0:
    print("Filtered Ethincs and Gender Count : ", count_df_eth_grp_mask)
    print("Filtered Ethincs Gender Data : ", df_eth_grp_mask, sep="\n")
else:
    print("Ethinicity Group and Gender wise Data breakup not available")

lev2_x = []
val_y = []

# Row by row iteration to gather data for plotting
for row in data['records']:
    if row['value'] != 'na':
        ##variables to have level_2 data
        lev2_x.append(row["level_2"])
        # variables to have Value data
        val_y.append(int(row["value"]))

print(lev2_x)
# ##user input for user age group
lower_age_group = input('enter the lower age group : ')
upper_age_group = input('enter the upper age group : ')
##spliting the lev2_x for check condition
if lower_age_group < upper_age_group:

        w = []
        for x in lev2_x:
            y = x.split(' ')
            w.append(y[0])

        i = []
        ##converting list string to list integer for check condition
        for wc in w:
            z = int(wc[0])
            if z <= 9:
                i.append(wc.zfill(2))

        f = i
        f1 = []
            ###check cond for user age group
        for i in f:
            if i >= lower_age_group and i <= upper_age_group:
                f1.append(i)

        res = [item.replace('00', '0').replace('05', '5') for item in f1]
        pattern = re.compile("|".join(res))
            ##initial filter data

        final = [i for i in lev2_x if pattern.match(i)]


            ##condition for user age group < =49
        if upper_age_group <= str(49):
                e = []
                for i in final:
                    y = i[0:2]
                    x = i[0:2].rstrip()
                    z = len(x)
                    w = int(z)
                    ##check string starts with 5
                    if x.startswith('5') and w == 2:
                        e.append(i)
                final_re = e

                for j in final_re:
                        ##filtered string starts with 5
                    final.remove(j)
                final_result = final


            # ##condition for user age group >49 and <=54
        elif upper_age_group > str(49) and upper_age_group <= str(54):
                wa = []
                # condition check for string 55
                x = [i for i in final if i[0:2] == '55']
                wa.append(x)
                ##converted nested list to single list
                wa1 = flatten(wa)

                for i in final:
                   w = i[0:2].rstrip()
                   if w.startswith('5') and w.endswith('0'):
                ##filtered string 55 if string starts with 50
                       res = [i for i in final if i not in wa1]

                final_result = res


        ##condition for user age group > =55
        elif upper_age_group >= str(55):
                final_result = final


# # # # # #find the length of final_result and generate and array
        pos_x = np.arange(len(final_result))
        val_y_re=[ rowe['value']  for rowe in data['records'] if rowe['level_2'] in final_result]

# # # # # #plotting the Graph

        plt.bar(pos_x, [int(x) for x in val_y_re], align='center', alpha=1.0)
        plt.xticks( pos_x,final_result, rotation='vertical')
        plt.ylabel('Registered Count')
        plt.title('Registration Data')
        plt.show()
else:
    raise ValueError('Lower range value for age group should be less than Upper age group')
