# pip install msal requests

import msal
import requests
import json

# Microsoft Graph API endpoint
graph_url = 'https://graph.microsoft.com/v1.0'

# App registration details
tenant_id = ""  # Your Office 365 tenant ID
client_id = ""  # App registration client ID
# client_secret = ""  # App registration secret
client_secret = ''


# Configure MSAL app
app = msal.ConfidentialClientApplication(
    client_id=client_id,
    client_credential=client_secret,
    authority=f"https://login.microsoftonline.com/{tenant_id}"
)

# Define the scopes we need
scopes = ['https://graph.microsoft.com/.default']

# Get token
result = app.acquire_token_for_client(scopes=scopes)

# Check if token was obtained
if "access_token" in result:
    access_token = result['access_token']
    print("Authentication successful!")
    
    # Set up headers for API calls
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    
    # Test connection - get organization details
    org_response = requests.get(f"{graph_url}/organization", headers=headers)
    
    if org_response.status_code == 200:
        org_data = org_response.json()
        print(f"Connected to Office 365 tenant: {org_data['value'][0]['displayName']}")
        
        # List users example
        print("\nUsers in the domain:")
        users_response = requests.get(
            f"{graph_url}/users?$select=displayName,userPrincipalName,id", 
            headers=headers
        )
        
        if users_response.status_code == 200:
            users = users_response.json()['value']
            for user in users:
                print(f"- {user['displayName']} ({user['userPrincipalName']})")
        else:
            print(f"Error listing users: {users_response.status_code}")
            print(users_response.text)
            
        # List groups example
        print("\nGroups in the domain:")
        groups_response = requests.get(
            f"{graph_url}/groups?$select=displayName,description,id", 
            headers=headers
        )
        
        if groups_response.status_code == 200:
            groups = groups_response.json()['value']
            for group in groups:
                print(f"- {group['displayName']}")
        else:
            print(f"Error listing groups: {groups_response.status_code}")
            print(groups_response.text)
            
    else:
        print(f"Error connecting to tenant: {org_response.status_code}")
        print(org_response.text)
        
else:
    print("Authentication failed:")
    print(result.get("error"))
    print(result.get("error_description"))
    print(result.get("correlation_id"))