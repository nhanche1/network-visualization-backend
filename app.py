from flask import Flask, request, send_file, Response
import csv
import math
from datetime import datetime
from collections import defaultdict, Counter
import zipfile
import os
import io

app = Flask(__name__)

# Logic từ mã Python gốc (được rút gọn và tái sử dụng)
def standardize_system_name(system):
    system = str(system).upper().strip()
    if '2G' in system or 'GSM' in system: return '2G'
    elif '3G' in system or 'UMTS' in system: return '3G'
    elif '4G' in system or 'LTE' in system: return '4G'
    elif '5G' in system or 'NR' in system: return '5G'
    return system

def standardize_frequency(freq, system):
    freq = str(freq).strip()
    system = standardize_system_name(system)
    if freq == 'n77': return '3800'
    cleaned_freq = ''.join(filter(str.isdigit, freq))
    if system == '2G':
        if cleaned_freq in ['900', '1800']: return cleaned_freq
        elif int(cleaned_freq) < 1000: return '900'
        else: return '1800'
    return cleaned_freq

def get_data_layer(data_usage, tech):
    data_usage = float(data_usage)
    tech = standardize_system_name(tech)
    if tech == '2G':
        if data_usage > 30: return 5
        elif data_usage > 20: return 4
        elif data_usage > 5: return 3
        elif data_usage > 1: return 2
        else: return 1
    elif tech == '3G':
        if data_usage > 5: return 5
        elif data_usage > 3: return 4
        elif data_usage > 1: return 3
        elif data_usage > 0.3: return 2
        else: return 1
    elif tech == '4G':
        if data_usage > 500: return 5
        elif data_usage > 200: return 4
        elif data_usage > 50: return 3
        elif data_usage > 10: return 2
        else: return 1
    else:  # 5G
        if data_usage > 10000: return 6
        elif data_usage > 1000: return 5
        elif data_usage > 5000: return 4
        elif data_usage > 200: return 3
        elif data_usage > 50: return 2
        else: return 1

def create_coverage_kml(csv_content):
    # Logic tạo KML từ mã gốc (rút gọn)
    sites = defaultdict(dict)
    cells = []
    with io.StringIO(csv_content) as f:
        reader = csv.DictReader(f)
        for row in reader:
            site_id = row['SITEID']
            lat = float(row['LAT'])
            lon = float(row['LONG'])
            if site_id not in sites:
                sites[site_id] = {'lat': lat, 'lon': lon}
            cells.append({
                'site_id': site_id, 'cell_name': row['CELLNAME'], 'system': row['SYS'],
                'frequency': row['ARFCN/UARFCN/EARFCN/NR-ARFCN'], 'azimuth': float(row['AZIMUTH']),
                'data_usage': float(row['DATA']), 'type': int(row['TYPE'])
            })

    kml_lines = [
        '<?xml version="1.0" encoding="UTF-8"?>\n',
        '<kml xmlns="http://www.opengis.net/kml/2.2">\n',
        '<Document>\n',
        f'<name>Network_Coverage_{datetime.now().strftime("%Y%m%d_%H%M")}</name>\n'
    ]
    # Thêm logic tạo KML đầy đủ từ mã gốc nếu cần
    kml_lines.append('</Document>\n</kml>\n')
    return ''.join(kml_lines)

def create_points_kml(csv_content, color, size, icon):
    sites = []
    with io.StringIO(csv_content) as f:
        reader = csv.DictReader(f)
        for row in reader:
            sites.append({'site_id': row['SITEID'], 'lat': float(row['LAT']), 'lon': float(row['LONG'])})

    kml_lines = [
        '<?xml version="1.0" encoding="UTF-8"?>\n',
        '<kml xmlns="http://www.opengis.net/kml/2.2">\n',
        '<Document>\n',
        f'<name>Network_Sites_{datetime.now().strftime("%Y%m%d_%H%M")}</name>\n',
        f'<Style id="customStyle">\n<IconStyle>\n<color>{color}</color>\n<scale>{size}</scale>\n',
        f'<Icon><href>http://maps.google.com/mapfiles/kml/shapes/{icon}.png</href></Icon>\n',
        '</IconStyle>\n</Style>\n'
    ]
    for site in sites:
        kml_lines.append(f'<Placemark>\n<name>{site["site_id"]}</name>\n')
        kml_lines.append(f'<Point>\n<coordinates>{site["lon"]},{site["lat"]},0</coordinates>\n</Point>\n')
        kml_lines.append('<styleUrl>#customStyle</styleUrl>\n</Placemark>\n')
    kml_lines.append('</Document>\n</kml>\n')
    return ''.join(kml_lines)

def convert_csv_to_clf(csv_content):
    clf_lines = []
    with io.StringIO(csv_content) as f:
        reader = csv.DictReader(f)
        for row in reader:
            line = (
                f"{row['MCCMNC']};{row['CELLID']};{row['LAC']};{row['TYPE']};{row['LAT']};{row['LONG']};"
                f"{row['POS-RAT']};{row['DESC']};{row['SYSCLF']};{row['CELLNAME']};{row['AZIMUTH']};"
                f"{row['ANT_HEIGHT']};{row['HBW']};{row['VBW']};{row['TILT']};{row['SITEID']}"
            )
            clf_lines.append(line)
    return '\n'.join(clf_lines)

@app.route('/coverage-kmz', methods=['POST'])
def coverage_kmz():
    file = request.files['file']
    csv_content = file.read().decode('utf-8-sig')
    kml_content = create_coverage_kml(csv_content)
    
    temp_kml = "temp.kml"
    output_kmz = io.BytesIO()
    with open(temp_kml, 'w', encoding='utf-8') as kml:
        kml.write(kml_content)
    with zipfile.ZipFile(output_kmz, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.write(temp_kml, arcname="doc.kml")
    os.remove(temp_kml)
    
    output_kmz.seek(0)
    return send_file(output_kmz, download_name=f"Network_Coverage_{datetime.now().strftime('%Y%m%d_%H%M')}.kmz", as_attachment=True)

@app.route('/points-kmz', methods=['POST'])
def points_kmz():
    file = request.files['file']
    csv_content = file.read().decode('utf-8-sig')
    color = request.form.get('color', 'ff00ff00')
    size = request.form.get('size', '1.0')
    icon = request.form.get('icon', 'placemark_circle')
    
    kml_content = create_points_kml(csv_content, color, size, icon)
    
    temp_kml = "temp.kml"
    output_kmz = io.BytesIO()
    with open(temp_kml, 'w', encoding='utf-8') as kml:
        kml.write(kml_content)
    with zipfile.ZipFile(output_kmz, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.write(temp_kml, arcname="doc.kml")
    os.remove(temp_kml)
    
    output_kmz.seek(0)
    return send_file(output_kmz, download_name=f"Network_Sites_{datetime.now().strftime('%Y%m%d_%H%M')}.kmz", as_attachment=True)

@app.route('/convert-clf', methods=['POST'])
def convert_clf():
    file = request.files['file']
    csv_content = file.read().decode('utf-8-sig')
    clf_content = convert_csv_to_clf(csv_content)
    
    return Response(clf_content, mimetype='text/plain', 
                    headers={"Content-Disposition": f"attachment;filename=Network_Data_{datetime.now().strftime('%Y%m%d_%H%M')}.clf"})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)