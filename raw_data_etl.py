"""ETL/zipping for environmental sensor directories"""
import datetime as dt
import subprocess
import sys
import os
import shutil
import tempfile
import logging

import azure.storage.blob as azureblob
import azure.storage.filedatalake as azurelake

import config

def get_filter_date():
        next_sunday_offset = dt.timedelta((12-dt.datetime.now().weekday()) % 7)
        end_date = dt.datetime.now() + next_sunday_offset
        start_date = end_date - dt.timedelta(days=7)
        filter_date = f"{start_date.strftime('%Y%m%d')}-{end_date.strftime('%Y%m%d')}"
        return filter_date
    
def get_sas_token():
    sas_token = azureblob.generate_account_sas(
        account_name="b2aistaging",
        account_key=config.AZURE_STORAGE_ACCESS_KEY,
        resource_types=azureblob.ResourceTypes(container=True, object=True),
        permission=azureblob.AccountSasPermissions(read=True, write=True, list=True),
        expiry=dt.datetime.now(dt.timezone.utc)
        + dt.timedelta(hours=1),
    )
    return sas_token
            
def execute_DICOM_export(input_file_path, output_folder_path ):
    # fetch each input file from the input file
    DICOM_executable_location = os.path.abspath("./DICOMOCTExport_2/DICOMOCTExport_2/DicomOctExport.exe")
    DICOM_args = f"-octa -enfaceSlabs -overlayDcm -segDcm -dcm"
    subprocess.call(args=[DICOM_executable_location, input_file_path, output_folder_path, DICOM_args], stdout=os.stdout)
    # ./DicomOctExport.exe INPUT_fda_FILEPATH  OUTPUT_folder_PATH -octa -enfaceSlabs -overlayDcm -segDcm -dcm

def maestro2_raw_download():
    """extracts maestro2 device files from the target sites"""
    filter_date = get_filter_date()
    project_name = "AI-READI"
    device_name = "Maestro2"
    sites = ["site-test"]

    sas_token = get_sas_token()

    # create datalake clients
    source_service_client = azurelake.FileSystemClient.from_connection_string(
        config.AZURE_STORAGE_CONNECTION_STRING,
        file_system_name="raw-storage")
    
    # fetch subdirectories for each site
    for site_name in sites:
        logging.info(f"getting maestro files for {site_name}")
        source_directory = f"{project_name}/{site_name}/{site_name}_{device_name}/{site_name}_{device_name}_{filter_date}"
        # create new directory for retaining latest maestro files; this will be used to remove files later so this will be returned
        maestro_latest_input_dir = os.path.abspath("./maestro2_input")
        maestro_latest_out_dir = os.path.abspath("./maestro2_output")
        os.mkdir(maestro_latest_input_dir)
        os.mkdir(maestro_latest_out_dir)
        for file in source_service_client.get_paths(path=f"{source_directory}/"):
            file_name = str(file.name)
            local_file_name = f"{file_name.rsplit(sep='/', maxsplit=-1)[-1]}"
            download_file_path = os.path.join(arch_directory_name, local_file_name)
            file_client = source_service_client.get_file_client(file_path=file_name)
            with open(download_file_path, "wb+") as download_file:
                download_file.write(file_client.download_file().readall())
                        

def archive_and_upload_completed_topcon():
    destination_service_client = azurelake.FileSystemClient.from_connection_string(
        config.AZURE_STORAGE_CONNECTION_STRING,
        file_system_name="stage-1-container")
    project_name = "AI-READI"
    device_name = "Maestro2"
    destination_directory = f"{project_name}/pooled-data/{device_name}"
    destination_container_client = destination_service_client.get_file_client(file_path=f"{destination_directory}/{zip_file_base_name}.zip")
    with open(file=archive, mode="rb") as f:
        destination_container_client.upload_data(f, overwrite=True)

def main():
    """script downloads maestro files to local, runs executable, then bundles output and uploads to data lake stage-1 container"""


if __name__ == "__main__":
    main()