import csv
import json
import os
import time
from asyncio import wait_for

from flask import Flask, render_template, request, redirect, url_for, send_file, after_this_request
from werkzeug.utils import secure_filename

app = Flask(__name__)

UPLOAD_FOLDER = 'uploads'
RESULT_FOLDER = 'results'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['RESULT_FOLDER'] = RESULT_FOLDER

# Ensure the upload and results folders exist
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(RESULT_FOLDER, exist_ok=True)

def csv_to_json(csv_file_path, json_output_file_path, skip_headers=True, json_input_file_path=None, action="replace"):
    data = []

    # Open the CSV file and read its content
    with open(csv_file_path, mode='r', newline='') as file:
        if skip_headers:
            csv_reader = csv.DictReader(file)
        else:
            # If no headers, treat the CSV as having no headers and create default headers
            csv_reader = csv.reader(file)
            csv_reader = [{"field_1": row[0], "field_2": row[1]} for row in csv_reader]

        # If skip_headers is True, headers are used from the CSV
        if skip_headers:
            headers = [header.strip() for header in csv_reader.fieldnames]

            # Iterate over each row in the CSV
            for row in csv_reader:
                entry = {
                    "owner": row[headers[0]].strip(),  # Use the first column for "owner"
                    "name": row[headers[1]].strip()    # Use the second column for "table name"
                }
                data.append(entry)
        else:
            # Process manually created dictionary (without headers)
            for row in csv_reader:
                entry = {
                    "owner": row["field_1"].strip(),  # Use default field names
                    "name": row["field_2"].strip()
                }
                data.append(entry)

    # If JSON input file is provided
    if json_input_file_path:
        with open(json_input_file_path, mode='r') as json_file:
            json_data = json.load(json_file)

        # Determine if we're merging or replacing
        if action == "merge":
            # Merge the new data with the existing 'explicit_included_tables'
            existing_tables = json_data['cmd.replication_definition']['tasks'][0]['source']['source_tables']['explicit_included_tables']
            merged_tables = existing_tables + data
            json_data['cmd.replication_definition']['tasks'][0]['source']['source_tables']['explicit_included_tables'] = merged_tables
        else:
            # Replace the existing 'explicit_included_tables' with the new data
            json_data['cmd.replication_definition']['tasks'][0]['source']['source_tables']['explicit_included_tables'] = data

        # Write the updated content to the output JSON file
        with open(json_output_file_path, mode='w') as file:
            json.dump(json_data, file, indent=4)
    else:
        # Write only the CSV data to a JSON file without modifying a JSON input
        with open(json_output_file_path, mode='w') as file:
            for i, entry in enumerate(data):
                json.dump(entry, file)
                if i < len(data) - 1:
                    file.write(',\n')
                else:
                    file.write('\n')

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/convert', methods=['POST'])
def convert():
    if 'csv_file' not in request.files:
        return redirect(request.url)

    csv_file = request.files['csv_file']
    json_input_file = request.files['json_input_file']
    skip_headers = 'skip_headers' in request.form
    action = request.form['action']  # Get the user's choice (merge or replace)

    if csv_file.filename == '':
        return redirect(request.url)

    # Save the uploaded files
    csv_filename = secure_filename(csv_file.filename)
    csv_path = os.path.join(app.config['UPLOAD_FOLDER'], csv_filename)
    csv_file.save(csv_path)

    if json_input_file and json_input_file.filename != '':
        json_input_filename = secure_filename(json_input_file.filename)
        json_input_path = os.path.join(app.config['UPLOAD_FOLDER'], json_input_filename)
        json_input_file.save(json_input_path)
    else:
        json_input_path = None

    # Set output file path
    json_output_file_path = os.path.join(app.config['RESULT_FOLDER'], 'output.json')

    # Run the conversion with the selected action (merge or replace)
    csv_to_json(csv_path, json_output_file_path, skip_headers, json_input_path, action)

    # After sending the file, delete uploaded and generated files
    @after_this_request
    def remove_files(response):
        try:
            os.remove(csv_path)  # Delete CSV file
            if json_input_path:
                os.remove(json_input_path)  # Delete JSON input file if provided
            #os.remove(json_output_file_path)  # Delete output JSON file
        except Exception as e:
            print(f"Error deleting file: {e}")
        return response

    # Send the resulting JSON file to the user
    return send_file(json_output_file_path, as_attachment=True)

if __name__ == '__main__':
    app.run(debug=True)
