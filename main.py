"""Extraction/vendor-executable/zipping for maestro2 files"""
import datetime as dt
import subprocess
import sys
import os
import zipfile
import tempfile

import azure.storage.filedatalake as azurelake

import config

def get_filter_date():
    """gets filter dates"""
    next_sunday_offset = dt.timedelta((12-dt.datetime.now().weekday()) % 7)
    end_date = dt.datetime.now() + next_sunday_offset
    start_date = end_date - dt.timedelta(days=7)
    filter_date = f"{start_date.strftime('%Y%m%d')}-{end_date.strftime('%Y%m%d')}"
    return filter_date

def execute_dicom_export(input_file_path, output_folder_path):
    """runs executable DicomOctExport.exe INPUT_fda_FILEPATH  OUTPUT_folder_PATH -octa -enfaceSlabs -overlayDcm -segDcm -dcm"""
    dicom_executable_location = os.path.abspath("./DICOMOCTExport_2/DICOMOCTExport_2/DicomOctExport.exe")
    dicom_args = "-octa -enfaceSlabs -overlayDcm -segDcm -dcm"
    # run executable
    subprocess.call(args=[dicom_executable_location, input_file_path, output_folder_path, dicom_args], stdout=sys.stdout)

def main():
    """script downloads maestro files to local, runs executable, then bundles output and uploads to data lake stage-1 container"""
    project_name = "AI-READI"
    site_names = ["site-test"]
    device_name = "Maestro2"
    filter_date = get_filter_date()

    dicom_executable_location = os.path.abspath("./DICOMOCTExport_2/DICOMOCTExport_2/DicomOctExport.exe")
    dicom_args = "-octa -enfaceSlabs -overlayDcm -segDcm -dcm"

    # create datalake clients
    source_service_client = azurelake.FileSystemClient.from_connection_string(
        config.AZURE_STORAGE_CONNECTION_STRING,
        file_system_name="raw-storage")
    destination_service_client = azurelake.FileSystemClient.from_connection_string(
        config.AZURE_STORAGE_CONNECTION_STRING,
        file_system_name="stage-1-container")
    
    # fetch subdirectories for each site
    for site_name in site_names:
        source_directory = f"{project_name}/{site_name}/{site_name}_{device_name}/{site_name}_{device_name}_{filter_date}"
        destination_directory = f"{project_name}/pooled-data/{device_name}"
        # create temp file to hold maestro2 input files
        for file in source_service_client.get_paths(path=f"{source_directory}/"):
            file_name = str(file.name)
            local_file_name = f"{file_name.rsplit(sep='/', maxsplit=-1)[-1]}"
            file_client = source_service_client.get_file_client(file_path=file_name)
            [maestro_input_file, maestro_input_file_path] = tempfile.mkstemp(suffix='.fda', prefix=local_file_name)
            with os.fdopen(fd=maestro_input_file, mode="wb") as fp:
                fp.write(file_client.download_file().readall())
            # os.close(maestro_input_file)
            with tempfile.TemporaryDirectory(delete=False) as maestro_output_dir:
                # run executable
                # execute_dicom_export(input_file_path=maestro_input_file_path, output_folder_path=maestro_output_dir)
                subprocess.call(args=[dicom_executable_location, maestro_input_file_path, maestro_output_dir, dicom_args], stdout=sys.stdout)
                # once complete, zip all contents of directory file
                zip_file_base_name = f"{site_name}_{device_name}_{site_name}_{device_name}_{filter_date}_{local_file_name}.zip"
                # archive = shutil.make_archive(base_name=zip_file_base_name,format="zip",base_dir=maestro_output_dir,root_dir=maestro_output_dir)
                with zipfile.ZipFile(file=zip_file_base_name,mode='w',compression=zipfile.ZIP_DEFLATED) as archive:
                    for (dir_path, dir_name, file_list) in os.walk(maestro_output_dir):
                        for file in file_list:
                            file_path = os.path.join(dir_path, file)
                            archive.write(filename=file_path, arcname=file)
                
                # upload to stage-1 container
                destination_container_client = destination_service_client.get_file_client(file_path=f"{destination_directory}/{zip_file_base_name}")
                with open(file=zip_file_base_name, mode="rb") as f:
                    destination_container_client.upload_data(f, overwrite=True)
                # os.remove(zip_file_base_name)
                # os.remove(maestro_input_file_path)

if __name__ == "__main__":
    main()