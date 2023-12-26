import requests
import streamlit as st

# Replace the URL with the actual URL of your API endpoint
api_url_1 = "http://127.0.0.1:8000/get_dispositions"
api_url_2 = "http://127.0.0.1:8000/get_disposition_states"
api_url_3 = "http://127.0.0.1:8000/get_area_states"
api_url_4 = "http://127.0.0.1:8000/get_counter_data/all"
api_url_5 = "http://127.0.0.1:8000/get_all_logs/all?start_index=20&num_rows=40"

# Replace the payload with the data you want to send in the request
payload = {
    "area_states": ["Washington", "California"],
    "dispositions": ["DNC", "NP"]
}

# Make a POST request to the API
response_1 = requests.post(api_url_1, json=payload)
response_2 = requests.get(api_url_2,)
response_3 = requests.get(api_url_3,)
response_4 = requests.get(api_url_4,)
response_5 = requests.get(api_url_5,)

# Check if the request was successful (status code 200)
if response_1.status_code == 200:
    # Print the response data (assuming it's in JSON format)
    st.write("Passing area states and disposition as parameters in api.")
    st.write(response_1.json())

    # Print the response data (assuming it's in JSON format)
    st.write("Getting unique dispositions.")
    st.write(response_2.json())

    # Print the response data (assuming it's in JSON format)
    st.write("Getting unique Area states.")
    st.write(response_3.json())

    # Print the response data (assuming it's in JSON format)
    st.write("Getting couter data against all states")
    st.write(response_4.json())
    
    # Print the response data (assuming it's in JSON format)
    st.write("Getting data against all states with starting index of 20 and getting 40 rows of data")
    st.write(response_5.json())
else:
    # Print an error message if the request was not successful
    print(f"Error: {response_1.status_code} - {response_1.text}")
