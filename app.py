from flask import Flask, request, send_file, Response, jsonify
import csv
import math
from datetime import datetime
from collections import defaultdict
import zipfile
import io
from flask_cors import CORS
from concurrent.futures import ThreadPoolExecutor

app = Flask(__name__)
CORS(app)

# Hàm phụ trợ
def process_site(row, sites_set):
    try:
        site_id = row['SITEID']
        lat = float(row['LAT'])
        lon = float(row['LONG'])
        note = row['NOTE']
        if site_id not in sites_set:
            sites_set.add(site_id)
            return {'site_id': site_id, 'lat': lat, 'lon': lon, 'note': note}
    except (KeyError, ValueError) as e:
        return None

def create_points_kml(csv_content, color, size, icon):
    sites = []
    sites_set = set()
    required_columns = {'SITEID', 'LAT', 'LONG', 'NOTE'}

    try:
        with io.StringIO(csv_content) as f:
            reader = csv.DictReader(f)
            if not required_columns.issubset(reader.fieldnames):
                missing = required_columns - set(reader.fieldnames)
                raise ValueError(f"Missing required columns: {missing}")

            with ThreadPoolExecutor() as executor:
                for site in executor.map(lambda row: process_site(row, sites_set), reader):
                    if site:
                        sites.append(site)

        if not sites:
            raise ValueError("No valid sites found in CSV.")

        kml_lines = [
            '<?xml version="1.0" encoding="UTF-8"?>\n',
            '<kml xmlns="http://www.opengis.net/kml/2.2">\n',
            '<Document>\n',
            f'<name>Network_Sites_{datetime.now().strftime("%Y%m%d_%H%M")}</name>\n',
            f'<Style id="customStyle">\n<IconStyle>\n<color>{color}</color>\n<scale>{size}</scale>\n',
            f'<Icon><href>http://maps.google.com/mapfiles/kml/shapes/{icon}.png</href></Icon>\n',
            '</IconStyle>\n</Style>\n',
            '<Folder>\n<name>Sites</name>\n<visibility>0</visibility>\n'
        ]

        for site in sites:
            kml_lines.append(f'<Placemark>\n<name>{site["site_id"]}</name>\n')
            kml_lines.append('<Snippet maxLines="0"></Snippet>\n')
            kml_lines.append('<description><![CDATA[\n')
            kml_lines.append(f'<h3>Thông tin Site</h3>\n')
            kml_lines.append(f'<p><b>SiteID:</b> {site["site_id"]}</p>\n')
            kml_lines.append(f'<p><b>Latitude:</b> {site["lat"]}</p>\n')
            kml_lines.append(f'<p><b>Longitude:</b> {site["lon"]}</p>\n')
            kml_lines.append(']]></description>\n')
            kml_lines.append('<styleUrl>#customStyle</styleUrl>\n')
            kml_lines.append(f'<Point>\n<coordinates>{site["lon"]},{site["lat"]},0</coordinates>\n</Point>\n')
            kml_lines.append('</Placemark>\n')

        kml_lines.append('</Folder>\n')
        kml_lines.append('</Document>\n</kml>\n')
        return ''.join(kml_lines)
    except Exception as e:
        raise Exception(f"Error creating KML: {str(e)}")

def convert_csv_to_clf(csv_content):
    required_columns = {'MCCMNC', 'CELLID', 'LAC', 'TYPE', 'LAT', 'LONG', 'POS-RAT', 
                        'DESC', 'SYSCLF', 'CELLNAME', 'AZIMUTH', 'ANT_HEIGHT', 'HBW', 
                        'VBW', 'TILT', 'SITEID'}
    
    clf_lines = []
    try:
        with io.StringIO(csv_content) as f:
            reader = csv.DictReader(f)
            if not required_columns.issubset(reader.fieldnames):
                missing = required_columns - set(reader.fieldnames)
                raise ValueError(f"Missing required columns: {missing}")
            
            for row in reader:
                try:
                    cell_id = row['CELLID']
                    if row['SYSCLF'] == '4':  # 4G
                        try:
                            eNodeB_id, cid = map(int, cell_id.split('-'))
                            cell_id = (eNodeB_id * 256) + cid
                        except (ValueError, AttributeError):
                            pass
                    line = (
                        f"{row['MCCMNC']};{cell_id};{row['LAC']};{row['TYPE']};{row['LAT']};{row['LONG']};"
                        f"{row['POS-RAT']};{row['DESC']};{row['SYSCLF']};{row['CELLNAME']};{row['AZIMUTH']};"
                        f"{row['ANT_HEIGHT']};{row['HBW']};{row['VBW']};{row['TILT']};{row['SITEID']}"
                    )
                    clf_lines.append(line)
                except Exception:
                    continue
        if not clf_lines:
            raise ValueError("No valid data to convert to CLF.")
        return '\n'.join(clf_lines)
    except Exception as e:
        raise Exception(f"Error converting to CLF: {str(e)}")

# API Endpoints
@app.route('/points-kmz', methods=['POST'])
def points_kmz():
    try:
        if 'file' not in request.files:
            return jsonify({"error": "No file uploaded"}), 400
        file = request.files['file']
        csv_content = file.read().decode('utf-8-sig')
        color = request.form.get('color', 'ff00ff00')
        size = request.form.get('size', '1.0')
        icon = request.form.get('icon', 'placemark_circle')
        
        # Tạo KML
        kml_content = create_points_kml(csv_content, color, size, icon)
        
        # Tạo file KMZ
        output_kmz = io.BytesIO()
        with zipfile.ZipFile(output_kmz, 'w', zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("doc.kml", kml_content)
        output_kmz.seek(0)
        
        return send_file(
            output_kmz,
            mimetype='application/vnd.google-earth.kmz',
            download_name=f"Network_Sites_{datetime.now().strftime('%Y%m%d_%H%M')}.kmz",
            as_attachment=True
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/convert-clf', methods=['POST'])
def convert_clf():
    try:
        if 'file' not in request.files:
            return jsonify({"error": "No file uploaded"}), 400
        file = request.files['file']
        csv_content = file.read().decode('utf-8-sig')
        clf_content = convert_csv_to_clf(csv_content)
        
        return send_file(
            io.BytesIO(clf_content.encode('utf-8')),
            mimetype='text/plain',
            download_name=f"Network_Data_{datetime.now().strftime('%Y%m%d_%H%M')}.clf",
            as_attachment=True
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
