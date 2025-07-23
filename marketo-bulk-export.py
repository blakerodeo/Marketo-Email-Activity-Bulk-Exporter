import requests
import pandas as pd
import os
from datetime import datetime, timedelta

# place filepath here
CSV_FILE = '/filepath/filename.csv'
ACTIVITY_ID = '1'

# get number of rows in the referenced file
def current_row_count():
    if not os.path.isfile(CSV_FILE):
        return 0
    return sum(1 for _ in open(CSV_FILE)) - 1

##TOKENING##
token_url = 'https://000-XXX-000.mktorest.com/identity/oauth/token'
auth = {
    'grant_type': 'client_credentials',
    'client_id': 'xxx',
    'client_secret': 'xxx'
}


# make the GET request
token_response = requests.get(token_url, params=auth)
token = token_response.json()['access_token']

# Print the response
print('Status Code:', token_response.status_code)
print('Response:', token_response.text)
print('Token:', token_response.json()['access_token'])


##NEXT PAGING TOKENING##
# set the starting daytime
page_url = 'https://000-XXX-000.mktorest.com/rest/v1/activities/pagingtoken.json'
page_params = {
    'sinceDatetime': '2021-01-01T00:00:00'
}

headers = {
    'Authorization': f'Bearer {token}'
}

# make the GET request
page_response = requests.get(page_url, headers=headers, params=page_params)
nextPageToken = page_response.json()['nextPageToken']

## TO CONTINUE A STOPPED RUN, YOU CAN UNCOMMENT AND PASTE IN THE LAST RETURNED NEXT PAGE TOKEN BELOW##
#nextPageToken = 'HXY2DHHG5MYNRVG733ZBTKVRS4TATFGO7HXXNKHOZP2T3HFDWAGQ===='


# print the response
print('Paging Status Code:', page_response.status_code)
print('Paging Response:', page_response.text)
print('Paging Token:', page_response.json()['nextPageToken'])


total_rows = current_row_count()
moreResult = True

##GET REQUEST UNTIL THERE HAVE BEEN 250,000 RESPONSED RECORDED##
while total_rows < 250000 and moreResult == True:
    ##GET ACTIVITIES##
    activities_url = 'https://000-XXX-000.mktorest.com/rest/v1/activities.json'
    activities_params = {
        'activityTypeIds': ACTIVITY_ID,
        'nextPageToken': nextPageToken
    }

    # GET request
    activities_response = requests.get(activities_url, headers=headers, params=activities_params)
    nextPageToken = activities_response.json()['nextPageToken']
    moreResult = activities_response.json()['moreResult']

    # > 4 to make sure the response has data
    if len(activities_response.json()) > 4:

        activities_result = activities_response.json()['result']

        # flattens the activity attributes into the same level as the meta data
        for item in activities_result:

            for i in range(len(item['attributes'])):

                item[item['attributes'][i]['name']] = item['attributes'][i]['value']

            del item['attributes']   

        df = pd.DataFrame(activities_result)

        # adds row to csv, if it is the first row it will use the keys in the key/values as the header rows
        if not df.empty:
                write_mode = 'a' if os.path.exists(CSV_FILE) else 'w'
                header = not os.path.exists(CSV_FILE)
                df.to_csv(CSV_FILE, mode=write_mode, header=header, index=False)
                #total_rows += len(df)

    # row count check
    total_rows = current_row_count()
    
    # Print the response
    print('Activities Status Code:', activities_response.status_code)
    print('Next Paging Token:', activities_response.json()['nextPageToken'])
    print('Row Count:', total_rows)


