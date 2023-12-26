import os
import shutil
from  datetime import datetime
import requests
import time
import json
def zip_folder(folder_path, output_path):
    """
    Creates a zip archive of the specified folder.

    Parameters:
    folder_path (str): The path to the folder to be zipped.
    output_path (str): The path (including filename) for the output zip file.
    """
    # Check if the folder exists
    if not os.path.isdir(folder_path):
        raise ValueError("The specified folder does not exist.")

    # Create a zip file from the entire folder
    shutil.make_archive(output_path, 'zip', folder_path)
    return f"{output_path}.zip"

def send_api_request(api_url,file_path):
    """
    Send a POST request to an API with given data and a file.

    Parameters:
    api_url (str): URL of the API endpoint.
    data (dict): Dictionary containing the data to be sent.
    file_path (str): Path to the file to be uploaded.
    """
    # Open the file in binary mode
    with open(file_path, 'rb') as file:
        # Define the files dictionary. The key is the name of the form field for the file.
        files = {'files': (file_path, file, 'application/zip')}
        # data = {"data": json.dumps(data)}
        # Send the POST request
        
        # headers = {"Content-Type": "application/json"}

        response = requests.post(api_url,files=files)


        # Check response status
        if response.status_code == 200:
            print("Successfully sent the request.")
            return response
        else:
            print(f"Failed to send request. Status code: {response.status_code}, Response: {response.text}")
            return None

def delete_folder(folder_path):
    """
    Delete a folder and its contents from the specified path.

    Parameters:
    folder_path (str): Path to the folder to be deleted.
    """
    try:
        # Check if the folder exists
        if shutil.os.path.exists(folder_path):
            # Delete the folder and its contents
            shutil.rmtree(folder_path)
            print(f"Folder '{folder_path}' has been deleted.")
        else:
            print(f"Folder '{folder_path}' does not exist.")
    except Exception as e:
        print(f"An error occurred: {e}")

def delete_file(file_path):
    """
    Delete a file from the specified file path.

    Parameters:
    file_path (str): Path to the file to be deleted.
    """
    try:
        # Check if the file exists
        if os.path.exists(file_path):
            # Delete the file
            os.remove(file_path)
            print(f"File '{file_path}' has been deleted.")
        else:
            print(f"File '{file_path}' does not exist.")
    except Exception as e:
        print(f"An error occurred: {e}")

# Get the directory of the files and the last directory name
directory_path = "MED"
last_directory_name = os.path.basename(directory_path)

zip_dir_path = "temp"





#List all versions of Bot Directories
bot_version_dirs = os.listdir(directory_path)
for version_dir in bot_version_dirs:

    #Check if the directory exists
    if not os.path.exists(zip_dir_path):
        #If it doesn't exists , then create it
        os.makedirs(zip_dir_path)
    
    version_dir_path = os.path.join(directory_path,version_dir)
    date_dirs = os.listdir(version_dir_path)
    # Convert string dates to datetime objects
    dates = [datetime.strptime(date_string, '%Y-%m-%d') for date_string in date_dirs]

    # Find the most recent date
    most_recent_date = max(dates)

    # Convert the most recent date back to string if needed
    most_recent_date_string = most_recent_date.strftime('%Y-%m-%d')

    #Create the Path for Date Directory
    date_dir_path = os.path.join(version_dir_path,most_recent_date_string)

    #List all directories and files inside this date directory
    mac_addresses_dirs = os.listdir(date_dir_path)

    #Go into each MAC Address Directory one by one
    for mac_address_dir in mac_addresses_dirs:

        #Create path for that MAC_Directory
        mac_adress_dir_path = os.path.join(date_dir_path,mac_address_dir)

        # Create a new file named after that MAC_Address_Directory. 
        # This file will have all logs stored which are being generated by Virtual Machine having that Mac Address
        mac_address_log_file = os.path.join(zip_dir_path, f"{mac_address_dir}.txt")

        # Merge all files in the directory
        with open(mac_address_log_file, 'w') as merged_file:
            # Iterate over each file in that Mac Address directory
            for filename in os.listdir(mac_adress_dir_path):

                # Construct the full file path
                file_to_merge = os.path.join(mac_adress_dir_path, filename)

                # Check if it's a file and not a directory
                if os.path.isfile(file_to_merge):
                    with open(file_to_merge, 'r') as file:
        #               Append the content of each file to the merged file
                        merged_file.write(file.read())
                        merged_file.write("\n")  # Optionally add a newline between files

            merged_file.close()
    
    zip_file_path = zip_folder(zip_dir_path,f"MED|{version_dir}|{most_recent_date_string}")
    print(zip_file_path)
   



    #API Request to Call Analysis Backend Server
    send_api_request("http://127.0.0.1:8000/uploadlogfiles",zip_file_path)

    time.sleep(10)

    #Now delete the zip file
    delete_file(zip_file_path)

    #Now delete the temp folder 
    delete_folder(zip_dir_path)

    time.sleep(30)

    break